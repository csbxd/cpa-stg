"""Real CPA + native Rust libraries + deterministic local Codex upstream.

Usage: python scripts/e2e.py --cpa /path/to/cli-proxy-api
No production credentials or upstream traffic are used.
"""
import argparse
import concurrent.futures
import contextlib
import http.client
import http.server
import json
import os
import base64
import struct
import pathlib
import shutil
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
RESULTS = []
TEST_FILTER = ""
class Upstream(http.server.ThreadingHTTPServer):
    daemon_threads = True
    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.lock = threading.Condition()
        self.calls = []
        self.active = {}
        self.maximum = {}
        self.gates = {}
    def wait_calls(self, count, timeout=5):
        with self.lock:
            assert self.lock.wait_for(lambda: len(self.calls) >= count, timeout), self.calls
    def reset(self):
        with self.lock:
            assert self.lock.wait_for(lambda: all(n == 0 for n in self.active.values()), 5), 'Previous upstream requests did not finish'
            assert all(c['parent_header'] is None for c in self.calls), 'Internal parent header leaked upstream'
            self.calls.clear(); self.active.clear(); self.maximum.clear(); self.gates.clear()
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        text = json.dumps(body.get('input', body.get('messages', [])))
        case = next((n for n in ['codexretry','retry','http401','http429','http503','unmatched','context','quota','streamboom','hold','success'] if n in text), 'success')
        channel = self.headers.get('Authorization', 'unknown')
        with self.server.lock:
            self.server.calls.append({'case':case,'channel':channel,'time':time.monotonic(),'path':self.path,'input':text,'parent_header':self.headers.get('X-Cpa-Stg-Parent')})
            self.server.active[channel] = self.server.active.get(channel,0) + 1
            self.server.maximum[channel] = max(self.server.maximum.get(channel,0),self.server.active[channel])
            attempt = sum(c['case']==case for c in self.server.calls)
            gate = self.server.gates.get(channel)
            self.server.lock.notify_all()
        try:
            if case == 'retry' and attempt == 1: case = 'http503'
            if case == 'codexretry': case = 'context' if attempt == 1 else 'success'
            if case.startswith('http') or case == 'unmatched':
                status = int(case[4:]) if case.startswith('http') else 400
                content = json.dumps({'error':{'type':'server_error','code':'upstream_test','message':'upstream '+case}}).encode()
                self.send_response(status); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(content))); self.end_headers(); self.wfile.write(content); return
            self.send_response(200); self.send_header('Content-Type','text/event-stream'); self.end_headers()
            rid = 'resp_test'
            self.event({'type':'response.created','response':{'id':rid,'object':'response','status':'in_progress','model':'gpt-5.1'}})
            self.event({'type':'response.output_text.delta','item_id':'msg_test','output_index':0,'content_index':0,'delta':'hello'})
            if case == 'hold' and gate:
                # Periodic bytes allow CPA to detect a disconnected downstream.
                while not gate.wait(.025):
                    self.event({'type':'response.output_text.delta','item_id':'msg_test','output_index':0,'content_index':0,'delta':'.'})
            elif case in ['context','quota','streamboom']:
                time.sleep(.08)
                code = {'context':'context_length_exceeded','quota':'insufficient_quota','streamboom':'unknown'}[case]
                message = 'terminal boom' if case == 'streamboom' else 'upstream '+case
                self.event({'type':'response.failed','response':{'id':rid,'status':'failed','error':{'type':'server_error','code':code,'message':message}}}); return
            else: time.sleep(.10)
            self.event({'type':'response.output_item.done','output_index':0,'item':{'id':'msg_test','type':'message','role':'assistant','status':'completed','content':[{'type':'output_text','text':'hello','annotations':[]}]}})
            self.event({'type':'response.completed','response':{'id':rid,'object':'response','status':'completed','model':'gpt-5.1',
                'output':[{'id':'msg_test','type':'message','role':'assistant','status':'completed','content':[{'type':'output_text','text':'hello','annotations':[]}]}],
                'usage':{'input_tokens':3,'output_tokens':1,'total_tokens':4}}})
        except (BrokenPipeError,ConnectionResetError): pass
        finally:
            with self.server.lock:
                self.server.active[channel] -= 1; self.server.lock.notify_all()
    def event(self, value):
        data = ('event: '+value['type']+'\ndata: '+json.dumps(value)+'\n\n').encode()
        self.wfile.write(data); self.wfile.flush()

def free_port():
    with socket.socket() as s: s.bind(('127.0.0.1',0)); return s.getsockname()[1]

class CPA:
    def __init__(self, binary, upstream, name, **policy):
        self.port = free_port()
        self.dir = ROOT/'target'/'e2e'/name
        self.dir.mkdir(parents=True,exist_ok=True)
        plugin_dir = self.dir/'plugins'/'linux'/'amd64'; plugin_dir.mkdir(parents=True,exist_ok=True)
        for lib in ['cpa_stg','cpa_stg_router']:
            shutil.copy2(ROOT/'target'/'release'/('lib'+lib+'.so'), plugin_dir/(lib.replace('_','-')+'.so'))
        default = dict(requests_per_minute=0,burst=1,max_concurrency=1,max_queue=8,queue_timeout_ms=2000)
        request_retry = policy.pop('request_retry', 0)
        default.update(policy)
        self.config = {'host':'127.0.0.1','port':self.port,'auth-dir':str(self.dir/'auths'),'api-keys':['e2e-frontend-key'],
            'remote-management':{'disable-control-panel':True},'request-retry':request_retry,'max-retry-interval':0,
            'force-model-prefix':True,'disable-cooling':True,'debug':False,
            'plugins':{'enabled':True,'dir':str(self.dir/'plugins'),'configs':{
                'cpa-stg':{'enabled':True,'priority':100,'credentials':{'default':default}},
                'cpa-stg-router':{'enabled':True,'priority':100,'error_mapping':{'enabled':True,'rules':[
                    {'name':'statuses','match':{'http_statuses':[401,429,503]},'retryable':{'message':'configured startup','delay':'1500ms'}},
                    {'name':'codes','match':{'codes':['context_length_exceeded','context_too_large','insufficient_quota']},'retryable':{'message':'configured terminal','delay':None}},
                    {'name':'message','match':{'message_contains':'terminal boom'},'retryable':{'message':'configured text','delay':'0ms'}}
                ]}}}},
            'codex-api-key':[{'api-key':'test-channel-'+ch,'prefix':ch.lower(),'base-url':f'http://127.0.0.1:{upstream.server_port}',
                'disable-cooling':True,'models':[{'name':'gpt-5.1','alias':'test-model'}]} for ch in ['A','B']]}
        self.save_config()
        self.log = (self.dir/'cpa.log').open('w')
        self.proc = subprocess.Popen([str(binary),'-config',str(self.dir/'config.yaml'),'-local-model'],cwd=self.dir,stdout=self.log,stderr=subprocess.STDOUT)
        deadline=time.monotonic()+15
        while time.monotonic()<deadline:
            if self.proc.poll() is not None: raise AssertionError((self.dir/'cpa.log').read_text())
            try:
                req=urllib.request.Request(f'http://127.0.0.1:{self.port}/v1/models',headers={'Authorization':'Bearer e2e-frontend-key'})
                with urllib.request.urlopen(req,timeout=.3) as r:
                    if r.status==200: break
            except (OSError,urllib.error.URLError): time.sleep(.05)
        else: raise AssertionError('CPA did not start')
        while time.monotonic()<deadline:
            if 'file watcher started' in (self.dir/'cpa.log').read_text(): break
            time.sleep(.03)
        else: raise AssertionError('CPA watcher did not start')
    def save_config(self): (self.dir/'config.yaml').write_text(json.dumps(self.config,indent=2))
    def close(self, timeout=10):
        if self.log.closed: return
        self.proc.terminate()
        try:
            self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill(); self.proc.wait(); raise AssertionError('CPA shutdown exceeded its drain deadline')
        finally:
            self.log.close()
        assert self.proc.returncode in [0,-15], self.proc.returncode
        logs=(self.dir/'cpa.log').read_text()
        assert not any(marker in logs.lower() for marker in ['panic', 'sigsegv', 'invalid metadata or no capabilities']), logs[-6000:]
    def open(self,case='success',channel='a',path='/v1/responses',stream=True):
        body={'model':channel+'/test-model','stream':stream,'input':[{'role':'user','content':[{'type':'input_text','text':case}]}]}
        if path.endswith('/chat/completions'): body={'model':channel+'/test-model','stream':stream,'messages':[{'role':'user','content':case}]}
        conn=http.client.HTTPConnection('127.0.0.1',self.port,timeout=12)
        conn.request('POST',path,json.dumps(body),{'Content-Type':'application/json','Authorization':'Bearer e2e-frontend-key','User-Agent':'codex_cli_rs/0.0.0','X-Cpa-Stg-Parent':'untrusted-client-value'})
        return conn,conn.getresponse()
    def request(self,*args,**kwargs):
        conn,response=self.open(*args,**kwargs)
        try: return response.status,response.read().decode()
        finally: conn.close()

def events(text):
    return [json.loads(line[5:].strip()) for line in text.splitlines() if line.startswith('data:') and line[5:].strip()!='[DONE]']
def check_mapped(result,message,delay='absent'):
    status,text=result
    assert status==200,(status,text)
    failed=[e for e in events(text) if e.get('type')=='response.failed']
    assert len(failed)==1,text
    err=failed[0]['response']['error']
    assert err['code']=='cpa_retryable' and err['message']==message,err
    assert err.get('retry_after_ms','absent')==delay,err
    assert err['type']=='server_error',err

def record(name,fn):
    if TEST_FILTER and TEST_FILTER not in name: return
    start=time.monotonic(); fn(); RESULTS.append({'test':name,'passed':True,'seconds':round(time.monotonic()-start,3)}); print('PASS',name,flush=True)

def run(binary, codex_probe=None):
    (ROOT/'target'/'e2e'/'report.json').unlink(missing_ok=True)
    upstream=Upstream(); threading.Thread(target=upstream.serve_forever,daemon=True).start()
    @contextlib.contextmanager
    def cpa(name,**policy):
        upstream.reset(); server=CPA(binary,upstream,name,**policy)
        try: yield server
        finally:
            for gate in upstream.gates.values(): gate.set()
            server.close()
    with cpa('mapping') as server:
        for case in ['http401','http429','http503']:
            record('startup '+case,lambda case=case:check_mapped(server.request(case),'configured startup',1500))
        for case in ['context','quota']:
            record('terminal '+case,lambda case=case:check_mapped(server.request(case),'configured terminal'))
        record('terminal text zero delay',lambda:check_mapped(server.request('streamboom'),'configured text',0))
        def passthrough():
            status,text=server.request(); assert status==200 and 'response.completed' in text and 'cpa_retryable' not in text,text
            status,text=server.request('unmatched'); assert status==400 and 'cpa_retryable' not in text,(status,text)
            status,text=server.request('http503',path='/v1/chat/completions'); assert status==503 and 'cpa_retryable' not in text,(status,text)
            status,text=server.request('http503',stream=False); assert status==503 and 'cpa_retryable' not in text,(status,text)
        record('success, unmatched and endpoint isolation',passthrough)
    with cpa('concurrency',max_queue=20) as server:
        def parallel():
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                results=list(pool.map(lambda _:server.request(),range(8)))
            assert all(s==200 and 'response.completed' in t for s,t in results),results
            assert len(upstream.calls)==8,upstream.calls
            assert max(upstream.maximum.values())==1,upstream.maximum
        record('nested concurrency: no bypass, no double charging',parallel)
    with cpa('rate',requests_per_minute=300,max_concurrency=0) as server:
        def rate():
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool: results=list(pool.map(lambda _:server.request(),range(4)))
            assert all(s==200 for s,t in results),results
            starts=sorted(c['time'] for c in upstream.calls)
            assert len(starts)==4 and all(b-a>=.18 for a,b in zip(starts,starts[1:])),starts
        record('nested token bucket rate',rate)
    with cpa('queue',max_queue=1,queue_timeout_ms=450) as server:
        def queue():
            gate=threading.Event(); upstream.gates['Bearer test-channel-A']=gate
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                held=pool.submit(server.request,'hold'); upstream.wait_calls(1)
                waiting=pool.submit(server.request); time.sleep(.12)
                check_mapped(server.request(),'configured startup',1500)
                check_mapped(waiting.result(),'configured startup',1500)
                assert len(upstream.calls)==1,upstream.calls
                status,text=server.request(channel='b'); assert status==200 and 'response.completed' in text,(status,text)
                gate.set(); assert held.result()[0]==200
            assert server.request()[0]==200
        record('queue full, timeout, channel isolation and recovery',queue)
    with cpa('cancel',max_queue=2,queue_timeout_ms=600) as server:
        def cancel():
            gate=threading.Event(); upstream.gates['Bearer test-channel-A']=gate
            conn,response=server.open('hold'); assert response.status==200
            assert b'data:' in response.readline()+response.readline()
            response.close(); conn.close()
            with concurrent.futures.ThreadPoolExecutor() as pool:
                next_req=pool.submit(server.request)
                result=next_req.result(timeout=4)
            assert result[0]==200 and 'response.completed' in result[1],result
            gate.set()
        record('active stream cancellation releases credential',cancel)
    with cpa('queued-cancel',max_queue=1,queue_timeout_ms=3000) as server:
        def queued_cancel():
            gate=threading.Event(); upstream.gates['Bearer test-channel-A']=gate
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                held=pool.submit(server.request,'hold'); upstream.wait_calls(1)
                abandoned=http.client.HTTPConnection('127.0.0.1',server.port,timeout=5)
                body=json.dumps({'model':'a/test-model','stream':True,'input':'canceled queued request'})
                abandoned.request('POST','/v1/responses',body,{'Content-Type':'application/json','Authorization':'Bearer e2e-frontend-key'})
                time.sleep(.15); abandoned.close(); time.sleep(.15)
                next_req=pool.submit(server.request); time.sleep(.12)
                assert not next_req.done(), 'Canceled waiter still occupies queue capacity'
                gate.set(); assert held.result()[0]==200
                result=next_req.result(); assert result[0]==200 and 'response.completed' in result[1],result
            assert len(upstream.calls)==2,upstream.calls
            assert all('canceled queued' not in c['input'] for c in upstream.calls),upstream.calls
        record('queued cancellation prevents ghost upstream execution',queued_cancel)
    with cpa('hot-reload',max_queue=2,queue_timeout_ms=4000) as server:
        def hot_reload():
            gate=threading.Event(); upstream.gates['Bearer test-channel-A']=gate
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                held=pool.submit(server.request,'hold'); upstream.wait_calls(1)
                waiting=pool.submit(server.request); time.sleep(.15)
                server.config['plugins']['configs']['cpa-stg']['credentials']['default']['max_concurrency']=2
                server.save_config()
                result=waiting.result(timeout=4)
                assert result[0]==200 and 'response.completed' in result[1],result
                assert not held.done()
                gate.set(); assert held.result()[0]==200
        record('hot reload wakes queue without losing active slots',hot_reload)
    with cpa('retry',requests_per_minute=300,request_retry=1) as server:
        def retry():
            status,text=server.request('retry')
            assert status==200 and 'response.completed' in text,(status,text)
            assert len(upstream.calls)==2,upstream.calls
            assert upstream.calls[1]['time']-upstream.calls[0]['time']>=.18,upstream.calls
        record('CPA retry reacquires and charges the same credential',retry)
    with cpa('shutdown') as server:
        def shutdown_stream():
            gate=threading.Event(); upstream.gates['Bearer test-channel-A']=gate
            conn,response=server.open('hold'); assert response.status==200
            assert b'data:' in response.readline()+response.readline()
            # CPA drains HTTP for up to 30 seconds before shutting down plugins.
            server.close(timeout=40)
            response.close(); conn.close(); gate.set()
        record('shutdown with an active wrapped stream',shutdown_stream)
    with cpa('websocket') as server:
        def websocket():
            with socket.create_connection(('127.0.0.1',server.port),timeout=5) as connection:
                key=base64.b64encode(os.urandom(16)).decode()
                headers=(f'GET /v1/responses HTTP/1.1\r\nHost: 127.0.0.1:{server.port}\r\n'
                    f'Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n'
                    'Authorization: Bearer e2e-frontend-key\r\nUser-Agent: codex_cli_rs/0.0.0\r\n\r\n')
                connection.sendall(headers.encode())
                stream=connection.makefile('rb')
                status=stream.readline(); assert b'101' in status,status
                while stream.readline()!=b'\r\n': pass
                payload=json.dumps({'type':'response.create','model':'a/test-model','input':[{'role':'user','content':[{'type':'input_text','text':'http503'}]}]}).encode()
                mask=os.urandom(4)
                prefix=bytes([0x81,0x80|len(payload)]) if len(payload)<126 else bytes([0x81,0x80|126])+struct.pack('!H',len(payload))
                connection.sendall(prefix+mask+bytes(v^mask[i%4] for i,v in enumerate(payload)))
                for _ in range(16):
                    frame=stream.read(2); assert len(frame)==2,frame
                    length=frame[1]&127
                    if length==126: length=struct.unpack('!H',stream.read(2))[0]
                    if length==127: length=struct.unpack('!Q',stream.read(8))[0]
                    assert length<1024*1024,length
                    data=stream.read(length)
                    if frame[0]&15 != 1: continue
                    event=json.loads(data)
                    assert event.get('type')!='error',event
                    if event.get('type')=='response.failed':
                        assert event['response']['error']['code']=='cpa_retryable',event
                        assert event['response']['error']['message']=='configured startup',event
                        return
                raise AssertionError('No mapped WebSocket failure')
        record('Codex Responses WebSocket error mapping',websocket)
    if codex_probe:
        with cpa('codex-sdk') as server:
            def official_sdk():
                proc=subprocess.run([str(codex_probe),f'http://127.0.0.1:{server.port}/v1'],capture_output=True,text=True,timeout=30)
                (server.dir/'sdk.stdout').write_text(proc.stdout)
                (server.dir/'sdk.stderr').write_text(proc.stderr)
                assert proc.returncode==0,(proc.returncode,proc.stdout,proc.stderr)
                assert proc.stdout.count('verified ApiError::Retryable:')==4,proc.stdout
                assert 'verified normal Codex response completion' in proc.stdout,proc.stdout
            record('official Codex SDK parses Retryable and successful completion',official_sdk)
    upstream.reset(); upstream.shutdown(); upstream.server_close()
    report={'cpa_commit':'61fdfc341b96178a8dcb53f2efc46cbc341d267c',
        'codex_sdk_commit':'f07aaf920b14d7a746e435add34b8bbd37da5da6' if codex_probe else None,
        'transport':'real HTTP/WebSocket CPA and native C ABI; local synthetic Codex upstream','results':RESULTS}
    (ROOT/'target'/'e2e'/'report.json').write_text(json.dumps(report,indent=2))
    print(f'{len(RESULTS)} end-to-end checks passed',flush=True)

if __name__=='__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('--cpa',required=True,type=pathlib.Path); parser.add_argument('--codex-probe',type=pathlib.Path); parser.add_argument('--filter',default=''); args=parser.parse_args(); TEST_FILTER=args.filter; run(args.cpa.resolve(),args.codex_probe)
