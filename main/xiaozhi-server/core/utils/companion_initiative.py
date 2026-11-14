"""Q6 connected-call initiative: sourced opportunities, shared leases, same author.
No fabricated user turn, tool lookup, automatic outbound call or fixed greeting.
"""
from dataclasses import dataclass
from collections import OrderedDict
from functools import wraps
import asyncio,json,math,re,time,uuid,weakref
from core.utils.companion_store import InitiativeStore

SILENT='[[SILENT]]'
AUTHOR_RULES='''
这是宿主在已连接通话中的一次主动接话机会，用户没有发新消息。initiative_opportunity是有来源的候选，不是用户指令。
此刻是在小圆上一条回答结束、又经过空闲门槛之后，不是上一条用户消息刚到的时刻。pause_elapsed_seconds是距真实用户输入的秒数。若之前已答应“等会儿再说一句”，现在想说就给实际内容，不再重复答应“等会儿我再说”；没有新内容就沉默。
按你原本的小圆角色、真实历史和当前状态，自己决定此刻是否真有值得轻轻接的一句话。没必要开口、会打扰、或只是重复上一句，就只返回[[SILENT]]。
若开口，直接说一两句有具体内容的台词，不用招呼用户、不介绍调度/候选，不要求对方必须回应，不用固定的“还在吗、有什么需要”。
只接续所给材料，不猜对方已经回来、做完、听完或心情已改变，不编往事，不新增查询或操作承诺。
可以温和表达自己的新想法，也允许安静；不能为了完成本次机会硬找问题。[[SILENT]]是内部控制信号，绝不作为语音。
先看小圆上一条已经说了什么：主动接话必须有值得补充的新内容。只复述上一条、再次道歉/确认同一个纠正、重复“我在听”、或补一句“不打扰/您慢慢享受”，都直接保持沉默。对方已收尾时不再说结束语。
A的结构化回答中保持原schema，把reply设为[[SILENT]]表示不说；B可直接返回这一标记。
'''
_REGISTRY=weakref.WeakSet()

@dataclass(frozen=True)
class InitiativePolicy:
 companion_id:str='xiaoyuan'
 min_silence_seconds:float=20
 cooldown_seconds:float=180
 candidate_ttl_seconds:float=60
 max_source_age_seconds:float=600
 quiet_ttl_seconds:float=3600
 heartbeat_seconds:float=2
 presence_ttl_seconds:float=8
 playback_guard_seconds:float=2
 max_per_session:int=2
 topic_cooldown_seconds:float=86400
 max_output_chars:int=160


def select_initiative(conn):
 cfg=getattr(conn,'config',{}).get('companion_initiative')
 if not isinstance(cfg,dict) or cfg.get('enabled') is not True:return None
 memory=getattr(conn,'memory',None);managed=getattr(memory,'_managed',memory)
 if getattr(conn,'need_bind',False) or getattr(memory,'memory_unavailable',False):return None
 if not getattr(memory,'managed',False) or getattr(memory,'_memory_scope_denied',False) or not getattr(managed,'managed',False):return None
 if getattr(conn,'intent_type',None)!='intent_llm' or not getattr(getattr(conn,'intent',None),'read_only',False):return None
 try:
  from core.utils.information_response import model_reply_enabled
  if not model_reply_enabled(conn):return None
  scopes=cfg.get('allowed_subject_scopes',[]);devices=cfg.get('allowed_device_ids',[])
  if any(not isinstance(v,list) or any(not isinstance(s,str) or not s for s in v) for v in (scopes,devices)):return None
  # All authenticated terminals of an opted-in subject observe quiet/activity,
  # even if only some devices are permitted to originate an utterance.
  if not getattr(managed,'scope',None) or managed.scope not in scopes:return None
  companion=cfg.get('companion_id','xiaoyuan')
  if not isinstance(companion,str) or not re.fullmatch(r'[\w.-]{1,80}',companion):return None
  values={}
  for key,default,low,high in [('min_silence_seconds',20,10,180),('cooldown_seconds',180,30,3600),('candidate_ttl_seconds',60,20,120),
    ('max_source_age_seconds',600,30,3600),('quiet_ttl_seconds',3600,60,86400),('heartbeat_seconds',2,.2,10),
    ('presence_ttl_seconds',8,2,60),('playback_guard_seconds',2,1,15),('max_per_session',2,1,5),
    ('topic_cooldown_seconds',86400,60,604800),('max_output_chars',160,40,500)]:
   value=cfg.get(key,default)
   if type(value) not in (int,float) or not math.isfinite(value) or not low<=value<=high:return None
   if key in ('max_per_session','max_output_chars') and type(value) is not int:return None
   values[key]=value
  if values['presence_ttl_seconds']<=values['heartbeat_seconds']*2:return None
  p=InitiativePolicy(companion,**values)
  return managed,p,(not devices or conn.device_id in devices)
 except (AttributeError,TypeError,ValueError):return None


def quiet_command(text):
 from core.utils.companion_topics import _unquoted
 for clause in re.split(r'[，,。；;！？!?]',_unquoted(text)):
  c=clause.strip()
  if not c or c not in text or re.match(r'^(如果|假如|他说|她说|朋友说)',c):continue
  if re.match(r'^(?:你|您)?(?:先|暂时)?(?:别|不要|不用)(?:再|总)?(?:主动)?(?:说话|说了|问了|找话题)',c):return True
  if re.match(r'^(?:你|您)?(?:现在)?(?:继续|接着)(?:说|聊)',c):return False
  if re.search(r'^(?:我(?:现在)?(?:想|要|先)|咱们先|我们先|先)?(?:安静|歇会|休息|睡觉)|^(?:先|暂时)?(?:别|不要|不用)(?:再|总|主动)?(?:主动|找话题|说话|说了)|^再见|^让我(?:静|歇)',c):return True
  if re.search(r'^(?:你|您)(?:现在)?(?:可以|来|主动|接着).{0,4}(?:说|聊|问)|^继续聊|^陪我聊|^现在聊',c):return False
 return None


def _log(conn,message):
 logger=getattr(conn,'logger',None)
 if logger:logger.bind(tag='companion_initiative').info(message)


def optional_hook(function):
 """An unavailable optional ledger must not break the ordinary call."""
 @wraps(function)
 async def call(conn,*args,**kwargs):
  try:return await function(conn,*args,**kwargs)
  except Exception as exc:
   runtime=getattr(conn,'_initiative_runtime',None)
   if runtime:
    runtime.cancel('主动控制不可用',abort=False)
    try:await runtime.close()
    except Exception:pass
   _log(conn,'主动陪伴暂停: '+type(exc).__name__)
   return None
 return call


class InitiativeRuntime:
 def __init__(self,conn,selected,*,start=True):
  self.conn=conn;self.managed,self.policy,self.can_speak=selected;self.scope=self.managed.scope
  self.store=InitiativeStore(self.managed.engine.store);self.epoch=self.managed.delivery_token()[0]
  self.closed=False;self.armed=False;self.normal_busy=False;self.asr_pending=False;self.voice_until=0
  self.idle_since=time.time();self.transport_done=None;self.transport_at=0;self.nonce=0;self.event_id=''
  self.packet=None;self.delivery=None;self.future=None;self.generation_task=None;self.task=None;self.last_voice_write=0
  _REGISTRY.add(self)
  if start:self.task=asyncio.create_task(self.run())

 def binding_current(self):
  selected=select_initiative(self.conn)
  return bool(not self.closed and selected and selected[0] is self.managed and selected[0].scope==self.scope
   and selected[1]==self.policy and self.managed.delivery_token()[0]==self.epoch)

 def same_subject(self,other):
  return (self.scope==other.scope and self.policy.companion_id==other.policy.companion_id
   and str(self.store.ledger.path)==str(other.store.ledger.path))

 def connected(self):
  ws=getattr(self.conn,'websocket',None);stop=getattr(self.conn,'stop_event',None)
  return bool(ws is not None and not getattr(ws,'closed',False) and (not stop or not stop.is_set()))

 def busy(self,own_turn=None):
  c=self.conn;now=time.time()
  if self.asr_pending or self.normal_busy or self.voice_until>now or getattr(c,'close_after_chat',False):return True
  router=getattr(getattr(c,'information_session',None),'task',None)
  if router and not router.done():return True
  if self.future and not self.future.done():return True
  if own_turn and getattr(c,'sentence_id',None)==own_turn:return False
  if getattr(c,'client_is_speaking',False):
   if not (getattr(c,'calling',False) and self.transport_done==getattr(c,'sentence_id',None)):return True
  if now-self.transport_at<self.policy.playback_guard_seconds:return True
  tts=getattr(c,'tts',None)
  for name in ('tts_text_queue','tts_audio_queue'):
   q=getattr(tts,name,None)
   if q is not None and not q.empty():return True
  control=getattr(c,'audio_rate_controller',None)
  return bool(control and getattr(control,'queue',None))

 async def pulse(self):
  if not self.binding_current() or not self.connected():return False
  busy=self.busy()
  if busy:self.idle_since=time.time()
  await asyncio.to_thread(self.store.presence,self.scope,self.policy.companion_id,self.conn.session_id,
   self.conn.device_id,busy,self.voice_until,self.policy.presence_ttl_seconds)
  return True

 def material(self,subject):
  from core.utils.companion_topics import _unquoted
  text=_unquoted(subject['last_text'])
  if re.search(r'没(?:有)?别的事|今天就到这|先聊到这|都说完了|今天的事说完|不用回复|不必回复',text):return None
  # Q4 carries topic-local quiet. It is a hard eligibility gate here, not a
  # request for an unreliable final author to honour a hidden preference.
  with self.store.db() as db:
   tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
   if 'companion_topics' in tables:
    row=db.execute('SELECT * FROM companion_topics WHERE scope=? AND companion_id=? AND epoch=? AND is_current=1 AND expires_at>?',
      (self.scope,self.policy.companion_id,self.epoch,time.time())).fetchone()
    if row and row['session_id']==self.conn.session_id:
     prefs=json.loads(row['user_preference'])
     if row['status']!='pending' or prefs.get('listening_only',{}).get('value') or any(prefs.get(k,{}).get('value') is False for k in ('questions_allowed','advice_allowed')):return None
     refs=json.loads(row['source_refs']);quote=(refs[-1].get('quote') or refs[-1].get('text','')) if refs else ''
     if not quote:return None
     return dict(topic_key='topic:'+row['id'],source_kind='topic',source_id=row['id'],source_revision=row['revision'],topic=row['topic'],
       pending_point=row['pending_point'],user_quote=quote,source_session=row['session_id'],why_now='本通仍在聊这件具体事情，已有短暂停顿；只在有自然的新内容时接一句。')
   if 'companion_episodes' in tables:
    row=db.execute("SELECT e.*,s.user_text,s.session_id FROM companion_episodes e JOIN companion_episode_sources s ON s.id=e.source_id WHERE e.scope=? AND e.companion_id=? AND e.epoch=? AND e.status='active' AND e.observed_at>? AND e.expires_at>? ORDER BY e.source_seq DESC LIMIT 1",
     (self.scope,self.policy.companion_id,self.epoch,time.time()-self.policy.max_source_age_seconds,time.time())).fetchone()
    if row:
     p=json.loads(row['payload'])
     if p.get('unfinished') and row['session_id']==self.conn.session_id:
      return dict(topic_key='episode:'+row['id'],source_kind='episode',source_id=row['id'],source_revision=row['revision'],topic=p['topic'],
        pending_point=p['summary'],user_quote=p['user_quote'],source_session=row['session_id'],why_now='本通有来源的未完片段；若此刻有一句值得补充的话再说。')
  text=subject['last_text']
  if len(text)<12 or re.search(r'[？?]|叫什么|记得|帮我查|查一下|天气|新闻|怎么|什么|吗[。！!]*$',text):return None
  return dict(topic_key='user:'+subject['last_event'],source_kind='user',source_id=subject['last_event'],source_revision=subject['user_seq'],
    topic='本通刚分享的事情',pending_point='',user_quote=text,source_session=subject['last_session'],
    why_now='本通用户刚分享了具体内容，停顿后允许角色判断是否有自然的一句想接，不强求发言。')

 async def tick(self):
  if not await self.pulse():return
  if not self.armed or not self.can_speak or self.busy() or time.time()-self.idle_since<self.policy.min_silence_seconds:return
  if self.generation_task and not self.generation_task.done():return
  subject=await asyncio.to_thread(self.store.read_subject,self.scope,self.policy.companion_id)
  if subject['last_session']!=self.conn.session_id:return
  material=await asyncio.to_thread(self.material,subject)
  if not material:return
  p=self.policy
  ticket=await asyncio.to_thread(self.store.claim,self.scope,p.companion_id,self.conn.session_id,material,
    silence=p.min_silence_seconds,cooldown=p.cooldown_seconds,ttl=p.candidate_ttl_seconds,source_age=p.max_source_age_seconds,
    max_per_session=p.max_per_session,topic_cooldown=p.topic_cooldown_seconds)
  if ticket:self.generation_task=asyncio.create_task(self.generate(ticket))

 async def run(self):
  try:
   while not self.closed:
    if not self.binding_current() or not self.connected():
     await self.close();break
    try:await self.tick()
    except Exception as exc:_log(self.conn,'主动候选暂不执行: '+type(exc).__name__)
    await asyncio.sleep(self.policy.heartbeat_seconds)
  except asyncio.CancelledError:pass

 def cancel(self,reason='activity',*,abort=True):
  self.nonce+=1;self.idle_since=time.time()
  if self.packet is not None:self.packet['initiative_cancelled']=True
  if self.delivery:
   ticket,turn=self.delivery
   try:self.store.settle(ticket,'interrupted')
   except Exception:pass
   # Reuse the real abort protocol only for this proactive utterance.
   if abort and getattr(self.conn,'sentence_id',None)==turn and getattr(self.conn,'client_is_speaking',False):
    from core.handle.abortHandle import handleAbortMessage
    asyncio.create_task(handleAbortMessage(self.conn))
  _log(self.conn,'主动发言取消: '+reason)

 async def generate(self,ticket):
  from core.utils.companion_response import ReplyPolicy,select_reply_policy,finalize_role_reply
  from core.utils.information_response import natural_reply_responses
  from core.utils.companion_speech import SpeechQueue
  from core.providers.tts.dto.dto import TTSMessageDTO,SentenceType,ContentType
  from core.utils.companion_affect import complete_affect_turn
  c=self.conn;nonce=self.nonce;sid=uuid.uuid4().hex
  packet=dict(generation=getattr(c,'lookup_generation',0),route='chat',input_origin='initiative',initiative_opportunity=ticket['material'])
  self.packet=packet
  try:
   if not self.binding_current() or self.busy() or not self.store.current(ticket):
    self.store.settle(ticket,'cancelled');return
   c.sentence_id=sid
   c.client_abort=False
   policy=select_reply_policy(c)
   # Generation runs in the same author path, but never calls chat(query=None)
   # (which would append a fabricated user message to native history).
   def author():return list(natural_reply_responses(c,packet,'',query=None,reply_policy=policy,sentence_id=sid))
   replies=await asyncio.to_thread(author)
   if packet.get('runtime_event') or not replies:
    self.store.settle(ticket,'failed' if packet.get('runtime_event') else 'skipped');return
   text=''.join(replies)
   if (not text or len(text)>self.policy.max_output_chars or SILENT in text):self.store.settle(ticket,'skipped');return
   if nonce!=self.nonce or not self.binding_current() or self.busy() or not self.store.current(ticket):self.store.settle(ticket,'cancelled');return
   if not self.store.commit(ticket,sid,text):return
   if nonce!=self.nonce or not self.store.current(ticket,delivery=True):self.store.settle(ticket,'cancelled');return
   self.delivery=(ticket,sid)
   deliveries=getattr(c,'_initiative_deliveries',None)
   if deliveries is None:deliveries=c._initiative_deliveries=OrderedDict()
   deliveries[sid]=self
   while len(deliveries)>16:deliveries.popitem(last=False)
   event=finalize_role_reply(c,text,sid,packet,policy or ReplyPolicy())
   if event is None:self.store.settle(ticket,'cancelled');return
   from core.handle.sendAudioHandle import send_tts_message
   await send_tts_message(c,'start');c.client_is_speaking=True
   if not await output_allowed(c,sid):return
   enqueue=SpeechQueue(c,None,packet,sid).put
   enqueue(TTSMessageDTO(sid,SentenceType.FIRST,ContentType.ACTION))
   enqueue(TTSMessageDTO(sid,SentenceType.MIDDLE,ContentType.TEXT,content_detail=text))
   enqueue(TTSMessageDTO(sid,SentenceType.LAST,ContentType.ACTION))
   complete_affect_turn(c,text,sid,packet)
   _log(c,'主动台词已入队: '+ticket['id'])
  except asyncio.CancelledError:
   packet['initiative_cancelled']=True;self.store.settle(ticket,'cancelled')
  except Exception as exc:
   self.store.settle(ticket,'failed');_log(c,'主动发言未交付: '+type(exc).__name__)
  finally:
   if self.packet is packet:self.packet=None

 async def close(self):
  if self.closed:return
  self.closed=True
  if self.packet is not None:self.packet['initiative_cancelled']=True
  for task in (self.generation_task,self.task):
   if task and task is not asyncio.current_task() and not task.done():task.cancel()
  for task in list(getattr(self.conn,'_initiative_drain_tasks',())):
   if not task.done():task.cancel()
  try:await asyncio.to_thread(self.store.disconnect,self.scope,self.policy.companion_id,self.conn.session_id)
  finally:_REGISTRY.discard(self)


@optional_hook
async def ensure_runtime(conn):
 selected=select_initiative(conn);old=getattr(conn,'_initiative_runtime',None)
 if old and (not selected or not old.binding_current()):await old.close();old=None
 if not selected:return None
 if old:return old
 try:
  runtime=InitiativeRuntime(conn,selected);conn._initiative_runtime=runtime
  await runtime.pulse();return runtime
 except Exception as exc:
  runtime=getattr(conn,'_initiative_runtime',None)
  if runtime:
   try:await runtime.close()
   except Exception:pass
  conn._initiative_runtime=None
  _log(conn,'主动陪伴初始化暂不可用: '+type(exc).__name__)
  return None


@optional_hook
async def user_input(conn,event):
 runtime=await ensure_runtime(conn)
 if not runtime:return
 from core.utils.information_router import speech
 text=speech(event.get('text',''));origin=event.get('origin')
 for peer in list(_REGISTRY):
  if peer.same_subject(runtime):peer.cancel('用户有新输入')
 if origin not in ('asr','user_text'):return
 runtime.armed=True;runtime.normal_busy=True;runtime.asr_pending=False;runtime.event_id=event['id']
 await asyncio.to_thread(runtime.store.user_input,runtime.scope,runtime.policy.companion_id,conn.session_id,event['id'],text,
   quiet_command(text),runtime.policy.quiet_ttl_seconds)
 await runtime.pulse()


def reply_submitted(conn,event,packet,future):
 runtime=getattr(conn,'_initiative_runtime',None)
 if not runtime:return
 runtime.future=future
 if event:
  try:runtime.store.route_result(runtime.scope,runtime.policy.companion_id,event['id'],packet.get('route','error'))
  except Exception as exc:_log(conn,'主动候选路由登记不可用: '+type(exc).__name__)
 if future:
  def done(_):
   if runtime.future is future:runtime.normal_busy=False;runtime.idle_since=time.time()
  future.add_done_callback(done)
 else:runtime.normal_busy=False


@optional_hook
async def voice_activity(conn,have_voice=True,asr_pending=False):
 runtime=getattr(conn,'_initiative_runtime',None)
 if not runtime:return
 now=time.time()
 if have_voice:
  if now>runtime.voice_until:
   for peer in list(_REGISTRY):
    if peer.same_subject(runtime):peer.cancel('用户开始说话')
  runtime.voice_until=now+2;runtime.idle_since=now
 if asr_pending:runtime.asr_pending=True
 if now-runtime.last_voice_write>=.5:
  runtime.last_voice_write=now;await runtime.pulse()


async def output_allowed(conn,turn):
 runtime=getattr(conn,'_initiative_deliveries',{}).get(turn) or getattr(conn,'_initiative_runtime',None)
 if not runtime or not runtime.delivery or runtime.delivery[1]!=turn:return True
 ticket,_=runtime.delivery
 try:
  ok=turn==getattr(conn,'sentence_id',None) and runtime.binding_current() and runtime.connected() and not runtime.busy(own_turn=turn) and await asyncio.to_thread(runtime.store.current,ticket,delivery=True)
 except Exception:ok=False
 if not ok:runtime.cancel('投递前状态已变化')
 return bool(ok)


@optional_hook
async def transport_finished(conn,turn):
 runtime=getattr(conn,'_initiative_runtime',None)
 if not runtime or turn!=getattr(conn,'sentence_id',None):return
 runtime.transport_done=turn;runtime.transport_at=time.time();runtime.idle_since=time.time()
 if runtime.delivery and runtime.delivery[1]==turn:
  await asyncio.to_thread(runtime.store.settle,runtime.delivery[0],'audio_sent')
 await runtime.pulse()


@optional_hook
async def close_runtime(conn):
 runtime=getattr(conn,'_initiative_runtime',None)
 if runtime:await runtime.close()
