const QS=new URLSearchParams(location.search);
const FEED_URL=QS.get('feed')||(location.hostname.endsWith('.static.hf.space')?'https://huggingface.co/buckets/weslo00700/btc5-feed/resolve/latest.json':null);
const LOCAL=!FEED_URL;
const IC={Live:'<path d="M3 12h4l3-8 4 16 3-8h4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>',
Trades:'<path d="M4 19V9m6 10V5m6 14v-7m4 7H2" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>',
History:'<circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" stroke-width="2"/><path d="M12 7v5l3 2" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>',
Model:'<path d="M12 3l8 4.5v9L12 21l-8-4.5v-9z M12 12l8-4.5M12 12v9M12 12L4 7.5" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linejoin="round"/>',
Settings:'<circle cx="12" cy="12" r="3" fill="none" stroke="currentColor" stroke-width="2"/><path d="M12 2v3m0 14v3M4.9 4.9l2.1 2.1m10 10 2.1 2.1M2 12h3m14 0h3M4.9 19.1 7 17m10-10 2.1-2.1" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>'};
const TABS=[['Live','Live'],['Trades','Trades'],['History','History'],['Model','Model & Agents'],['Settings','Settings']];
let S=null,C=null,TR=null,EQ=null,HIST=null,PERF=null,BT=null,MODELS=null,WAL=null,TF='1m',tab=localStorage.btc5tab||'Live',tfil='all',skew=0,feedAge=null,busy=false,lastErr=null;
const $=id=>document.getElementById(id),esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const f2=(x,d=2)=>x==null||isNaN(x)?'—':Number(x).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});
const usd=(x,d=2)=>x==null||isNaN(x)?'—':(x<0?'−$':'$')+f2(Math.abs(x),d);
const sgn=(x,d=2)=>x==null||isNaN(x)?'—':(x>0?'+':x<0?'−':'')+'$'+f2(Math.abs(x),d);
const pct=(x,d=1)=>x==null||isNaN(x)?'—':(x*100).toFixed(d)+'%';
const cls=x=>x>0?'up':x<0?'dn':'';
const tET=(ts,s=true)=>ts?new Date(ts*1000).toLocaleTimeString('en-US',{timeZone:'America/New_York',hour:'numeric',minute:'2-digit',second:s?'2-digit':undefined})+' ET':'—';
const dET=ts=>ts?new Date(ts*1000).toLocaleString('en-US',{timeZone:'America/New_York',month:'short',day:'numeric',hour:'numeric',minute:'2-digit'})+' ET':'—';
const mmss=s=>{s=Math.max(0,Math.ceil(s));return Math.floor(s/60)+':'+String(s%60).padStart(2,'0')};
const dcl=d=>d==='UP'?'up':d==='DOWN'?'dn':'';
const now=()=>Date.now()/1000+skew;
const CONF=['VERY LOW','LOW','MODERATE','HIGH','VERY HIGH'];
const arrow=d=>d==='UP'?'<svg viewBox="0 0 24 24"><path d="M12 4l8 9h-5v7H9v-7H4z" fill="currentColor"/></svg>':'<svg viewBox="0 0 24 24"><path d="M12 20l-8-9h5V4h6v7h5z" fill="currentColor"/></svg>';
const ITV=()=>LOCAL&&S&&S.interactive!==false;
function toast(m,bad){const t=$('toast');t.textContent=m;t.style.borderColor=bad?'rgba(255,77,94,.5)':'';t.classList.add('on');clearTimeout(toast.t);toast.t=setTimeout(()=>t.classList.remove('on'),3200)}
// ---------------------------------------------------------------- nav
$('tabs').innerHTML=TABS.map(([k,l])=>`<button data-t="${k}">${l}</button>`).join('');
$('bnav').innerHTML=TABS.map(([k,l])=>`<button data-t="${k}"><svg viewBox="0 0 24 24">${IC[k]}</svg>${k==='Model'?'Model':l}</button>`).join('');
function show(t){if(!TABS.some(x=>x[0]===t))t='Live';tab=t;localStorage.btc5tab=t;document.querySelectorAll('[data-t]').forEach(b=>b.classList.toggle('on',b.dataset.t===t));
 document.querySelectorAll('section[data-tab]').forEach(s=>s.classList.toggle('hide',s.dataset.tab!==t));window.scrollTo({top:0});render(true);loadTab()}
['tabs','bnav'].forEach(id=>$(id).onclick=e=>{const b=e.target.closest('[data-t]');if(b)show(b.dataset.t)});
$('tfseg').onclick=e=>{const b=e.target.closest('[data-tf]');if(!b||!LOCAL)return;TF=b.dataset.tf;[...$('tfseg').children].forEach(x=>x.classList.toggle('on',x===b));loadCandles()};
$('tfilter').onclick=e=>{const b=e.target.closest('[data-f]');if(!b)return;tfil=b.dataset.f;[...$('tfilter').children].forEach(x=>x.classList.toggle('on',x===b));loadTrades()};
// ---------------------------------------------------------------- data
async function j(u,o){const r=await fetch(u,Object.assign({cache:'no-store'},o||{}));const txt=await r.text();let d;try{d=JSON.parse(txt)}catch(e){d={detail:txt}}if(!r.ok){const e=new Error(d.detail||d.error||r.status);e.status=r.status;throw e}return d}
async function post(u,body,retry){const h={'Content-Type':'application/json'};const tk=sessionStorage.btc5tok;if(tk)h['X-BTC5-Token']=tk;
 try{return await j(u,{method:'POST',headers:h,body:JSON.stringify(body||{})})}catch(e){if(e.status===401&&!retry){const ok=await askToken();if(ok)return post(u,body,true)}throw e}}
async function tick(){
 try{
  if(LOCAL){S=await j('/api/state');skew=S.server_time-Date.now()/1000;feedAge=0}
  else{const d=await j(FEED_URL+'?t='+Date.now());S=d.state;C=d.candles;PERF=d.performance;BT=d.backtest;HIST=d.history;MODELS=d.models;TR=d.trades||(S.trading&&S.trading.recent)||[];EQ=d.equity||null;feedAge=Date.now()/1000-d.pushed_at;skew=S.server_time-d.pushed_at}
  lastErr=null;render();
 }catch(e){lastErr=(LOCAL?'Server unreachable: ':'Feed unreachable: ')+e.message;banners()}
}
async function loadCandles(){if(!LOCAL)return;try{C=await j(`/api/candles?tf=${TF}&n=${TF==='1s'?300:90}`);if(tab==='Live')drawChart()}catch(e){}}
async function loadTrades(){if(LOCAL){try{[TR,EQ]=await Promise.all([j('/api/trading/trades?limit=200'+(tfil!=='all'?'&mode='+tfil:'')),j('/api/trading/equity?mode='+((S&&S.trading&&S.trading.mode)||'demo'))])}catch(e){return}}if(tab==='Trades')trades()}
async function loadHist(){if(LOCAL){try{[HIST,PERF]=await Promise.all([j('/api/history?limit=150'),j('/api/performance')])}catch(e){return}}if(tab==='History')history();if(tab==='Live')quick()}
async function loadModel(){if(LOCAL){try{const [b,m]=await Promise.all([j('/api/backtest'),j('/api/models')]);BT=b.latest;MODELS=m}catch(e){}}if(tab==='Model')model()}
async function loadWallet(){if(LOCAL){try{WAL=await j('/api/trading/wallet')}catch(e){}}if(tab==='Settings')wallet()}
function loadTab(){if(tab==='Trades')loadTrades();if(tab==='History')loadHist();if(tab==='Model')loadModel();if(tab==='Settings')loadWallet();if(tab==='Live')loadCandles()}
// ---------------------------------------------------------------- render
function render(force){if(!S)return;header();banners();if(tab==='Live')live();if(tab==='Settings')settings(force);if(tab==='Model'&&force)model();if(tab==='Trades'&&(force||!TR))trades();if(tab==='History'&&force)history()}
const T=()=>S.trading||{mode:'demo',auto_trade:false,limits:{},live:{}};
function header(){const r=S.round,h=S.health,tr=T();
 const fresh=['binance','coinbase','kraken','chainlink'].filter(n=>h.feeds[n]&&h.feeds[n].fresh).length;
 $('feedDot').className='d '+(!LOCAL&&feedAge>60?'bad':fresh===4?'ok':fresh?'wn':'bad');
 $('pxTop').innerHTML=r.price?'<b class="lab">BTC </b>$'+f2(r.price,0):'—';
 [...$('modeSeg').children].forEach(b=>{b.classList.toggle('on',b.dataset.m===tr.mode);b.disabled=!ITV()});
 const a=$('autoT');a.checked=!!tr.auto_trade;a.disabled=!ITV();const a2=$('autoT2');if(a2){a2.checked=!!tr.auto_trade;a2.disabled=!ITV()}
 $('estop').classList.toggle('hide',tr.mode!=='live');document.title=(r.prediction&&r.prediction.prediction?r.prediction.prediction+' · ':'')+(r.price?'$'+f2(r.price,0)+' · ':'')+'BTC-5'}
function banners(){const m=[];
 if(lastErr)m.push(['bad',lastErr]);
 if(S){const tr=T(),h=S.health;
  if(tr.mode==='live')m.push(['live',`LIVE TRADING — real money. Stake ${usd(tr.limits.stake_usd)} · daily loss cap ${usd(tr.limits.max_daily_loss_usd)} · max exposure ${usd(tr.limits.max_exposure_usd)}. Press STOP to halt instantly.`]);
  if(!LOCAL&&feedAge>60)m.push(['bad',`STALE — this view's data is ${Math.round(feedAge)}s old. The host may be paused or restarting; values are not live.`]);
  if(h&&!h.ok)m.push(['bad','Degraded: '+(h.clock.synced?'':'clock not synchronised · ')+Object.values(h.feeds).filter(f=>f.kind==='ws'&&!f.fresh).map(f=>f.name+' '+f.status).join(', ')]);
  if(!LOCAL)m.push(['info','View-only phone page: live predictions, demo balance and trades from the always-on feed. Trading controls (Demo/Live switch, stake, limits) work on the private BTC-5 app.'])}
 $('banners').innerHTML=m.map(([c,t])=>`<div class="banner ${c}">${esc(t)}</div>`).join('')}
// ---------------------------------------------------------------- live
function live(){const r=S.round,t=now(),el=t-r.start_ts,p=r.prediction,C2=552.92;
 $('rid').textContent=tET(r.start_ts,false)+' round';
 const fr=Math.min(1,Math.max(0,el/300));$('ringP').setAttribute('stroke-dasharray',`${(fr*C2).toFixed(1)} ${C2}`);
 if(el<90){$('cdl').textContent='Locks in';$('cd').textContent=mmss(90-el);$('cd2').textContent='closes in '+mmss(300-el)}
 else if(el<300){$('cdl').textContent='Closes in';$('cd').textContent=mmss(300-el);$('cd2').textContent='locked at 1:30'}
 else{$('cdl').textContent='Next round';$('cd').textContent='…';$('cd2').textContent=''}
 let ph=r.phase||'—';if(el>=300)ph='AWAITING NEXT ROUND';const pc=ph.includes('LOCKED')?'LOCKED':ph.includes('COMPLETED')?'COMPLETED':ph==='MISSED'?'MISSED':'';
 $('phase').className='phase '+pc;$('phase').innerHTML=(pc?'':'<span class="pulse"></span>')+esc(ph.replace('PREDICTION ',''));$('stage').textContent=p?'':(r.stage||'');
 $('px').textContent=r.price?'$'+f2(r.price):'—';$('pxsrc').textContent=r.price_source==='chainlink'?'Chainlink · resolution source':(r.price_source||'unavailable');
 const d=r.price&&r.open_price?r.price-r.open_price:null;
 $('diff').innerHTML=d==null?'<span class="mut">Waiting for the official opening price</span>':`<span class="${d>=0?'up':'dn'}" style="font-weight:700">${d>=0?'▲ +':'▼ −'}$${f2(Math.abs(d))}</span> <span class="mut">vs open $${f2(r.open_price)} · spot currently ${d>=0?'UP':'DOWN'}</span><div class="lbl">Polymarket settles on Chainlink's 60-second average (TWAP), so a near-flat spot move can still resolve the other way.</div>`;
 const m=r.market;$('mkt').innerHTML=m?`<span>Market</span><span>${esc(m.question.replace('Bitcoin Up or Down - ',''))}</span><span>Window</span><span>${tET(m.start_ts,false)} → ${tET(m.end_ts,false)}</span><span>Crowd price (UP)</span><span class="num">${r.poly_book&&r.poly_book.mid!=null?f2(r.poly_book.mid*100,1)+'¢':'—'}</span><span>Resolves on</span><span>Chainlink BTC/USD</span>`:`<span>Market</span><span class="wn">${esc(r.market_error||'detecting…')}</span>`;
 const ca=S.model.confidence_accuracy||{};
 if(!p){$('pbody').innerHTML=`<div class="lbl">PREDICTION</div><div class="dir wait mt8">${el<90?'Analyzing the round…<br><span class="mut" style="font-size:15px">The UP / DOWN call is published at exactly 1:30 (${tET(r.decision_ts_target)}).</span>':'No prediction recorded for this round.'}</div>`}
 else if(p.status==='MISSED'){$('pbody').innerHTML=`<div class="lbl">PREDICTION</div><div class="dir dn mt8" style="font-size:52px">MISSED</div><div class="sub mt8">${esc(p.missed_reason)}</div>`}
 else{const pd=p.prediction==='UP'?p.p_up:p.p_down,ci=CONF.indexOf(p.confidence),acc=ca[p.confidence];
  $('pbody').innerHTML=`<div class="row" style="justify-content:space-between"><span class="lbl">PREDICTION · LOCKED ${tET(p.decision_ts)}</span><span class="lbl num">${p.deviation_ms>=0?'+':''}${f2(p.deviation_ms,1)} ms</span></div>
  <div class="dir ${dcl(p.prediction)} mt8">${arrow(p.prediction)}${p.prediction}</div>
  <div class="row mt8" style="gap:8px"><span class="num" style="font-size:22px;font-weight:700">${pct(pd)}</span><span class="sub">${esc(p.confidence)} confidence</span><span class="conf">${CONF.map((c,i)=>`<i class="${i<=ci?'on':''}"></i>`).join('')}</span></div>
  <div class="pbar"><i style="width:${(p.p_up*100).toFixed(1)}%"></i></div><div class="plab"><span class="up">UP ${pct(p.p_up)}</span><span class="dn">DOWN ${pct(p.p_down)}</span></div>
  ${acc?`<div class="note">This confidence level was right ${pct(acc.accuracy)} of the time in ${acc.n.toLocaleString()} out-of-sample backtest rounds.</div>`:''}
  ${p.official_outcome?`<div class="res"><span>Official result <span class="${dcl(p.official_outcome)}">${p.official_outcome}</span></span><span class="badge ${p.result}">${p.result}</span></div>${twapNote(p)}`:`<div class="res"><span class="mut">Awaiting official Polymarket result · ${tET(p.end_ts,false)}</span><span class="badge">PENDING</span></div>`}`}
 strip();curTrade();recent();if(!live.q||Date.now()-live.q>5000){live.q=Date.now();quick()}
 if(!LOCAL&&(!live.c||Date.now()-live.c>4000)){live.c=Date.now();drawChart()}}
function strip(){const tr=T(),lim=tr.limits||{};const cur=tr.current;
 $('strip').innerHTML=`<div class="stat"><div class="lbl">Mode</div><div class="v"><span class="badge ${tr.mode}" style="font-size:13px;padding:5px 11px">${tr.mode==='live'?'● LIVE':'DEMO'}</span></div><div class="n">${tr.auto_trade?'Auto-trade ON · '+usd(lim.stake_usd,0)+'/round':'Auto-trade OFF'}</div></div>
 <div class="stat"><div class="lbl">${tr.mode==='live'?'Wallet balance':'Demo balance'}</div><div class="v num">${usd(tr.balance)}</div><div class="n">${tr.mode==='demo'?'started at '+usd(tr.demo_bankroll,0):esc(tr.balance_source||'')}</div></div>
 <div class="stat"><div class="lbl">Today's P&amp;L</div><div class="v num ${cls(tr.today_pnl)}">${sgn(tr.today_pnl)}</div><div class="n">all-time ${sgn(tr.realized_pnl)}</div></div>
 <div class="stat"><div class="lbl">Win rate</div><div class="v num">${pct(tr.win_rate)}</div><div class="n">${tr.wins||0} won · ${tr.losses||0} lost</div></div>
 <div class="stat"><div class="lbl">This round</div><div class="v" style="font-size:18px">${cur?tradeShort(cur):'<span class="mut">—</span>'}</div><div class="n">${cur?esc(cur.status==='SKIPPED'?(cur.error||'').slice(0,60):cur.status==='OPEN'?'open position':cur.status):'trades at the 1:30 lock'}</div></div>`}
function tradeShort(t){if(t.status==='SKIPPED')return '<span class="badge SKIPPED">SKIPPED</span>';if(t.status==='FAILED'||t.status==='UNFILLED')return `<span class="badge FAILED">${t.status}</span>`;
 if(t.status==='PENDING'||t.status==='SUBMITTING')return '<span class="badge PENDING">PLACING…</span>';
 return `<span class="${dcl(t.side)}">${t.side}</span> <span class="num">${usd((t.cost||0)+(t.fee||0),0)} @ ${f2((t.entry_price||0)*100,0)}¢</span>`+(t.pnl!=null?` <span class="num ${cls(t.pnl)}">${sgn(t.pnl)}</span>`:'')}
function curTrade(){const tr=T(),t=tr.current,r=S.round;$('trMode').innerHTML=`<span class="badge ${tr.mode}">${tr.mode.toUpperCase()}</span>`;
 if(!t||t.round_id!==r.round_id){const el=now()-r.start_ts;$('curTrade').innerHTML=`<div class="sub">${el>=91&&r.prediction?'No trade was placed for this round'+(tr.auto_trade?' (the prediction locked before auto-trade could act).':' — auto-trade was off.')+' The next trade is placed at the next 1:30 lock.':tr.auto_trade?`A ${tr.mode} trade of <b>${usd((tr.limits||{}).stake_usd,0)}</b> will be placed on the predicted side at the 1:30 lock (${tET(r.decision_ts_target)}), at the real Polymarket ask price.`:'Auto-trade is off — turn it on in the top bar to trade every round.'}</div>`;return}
 if(t.status==='SKIPPED'||t.status==='FAILED'||t.status==='UNFILLED'){$('curTrade').innerHTML=`<div class="row"><span class="badge ${t.status}">${t.status}</span><span class="badge ${t.mode}">${t.mode}</span></div><div class="sub mt8">${esc(t.error)}</div>`;return}
 const win=t.shares,pay=t.status==='WON'?t.payout:null;
 $('curTrade').innerHTML=`<div class="row" style="justify-content:space-between"><div class="dir ${dcl(t.side)}" style="font-size:38px">${arrow(t.side)}${t.side}</div><span class="badge ${t.status}">${t.status}</span></div>
 <div class="kv mt"><span>Stake</span><span class="num">${usd((t.cost||0)+(t.fee||0))}</span><span>Entry price</span><span class="num">${f2((t.entry_price||0)*100,1)}¢ <span class="mut">(best ask ${f2((t.best_ask||0)*100,0)}¢)</span></span><span>Shares</span><span class="num">${f2(t.shares,2)}</span><span>Pays if ${t.side}</span><span class="num up">${usd(win)}</span>${t.fee?`<span>Polymarket fee</span><span class="num">${usd(t.fee)}</span>`:''}${t.pnl!=null?`<span>Result</span><span class="num ${cls(t.pnl)}">${sgn(t.pnl)}</span>`:''}</div>
 <div class="note">${esc(t.price_source||'')}</div>`}
function recent(){const H=(S.trading&&S.trading.recent)||[];
 $('recent').innerHTML=H.slice(0,6).map(t=>`<div class="tcard" style="padding:9px 0"><div><div style="font-weight:650">${tET(t.start_ts,false)} <span class="${dcl(t.side)}">${t.side||''}</span></div><div class="lbl">${t.mode} · ${t.status==='SKIPPED'?'skipped':usd((t.cost||0)+(t.fee||0),0)+' @ '+f2((t.entry_price||0)*100,0)+'¢'}</div></div><div style="text-align:right"><span class="badge ${t.status}">${t.status}</span><div class="num ${cls(t.pnl)}" style="font-size:12.5px;margin-top:3px">${t.pnl!=null?sgn(t.pnl):''}</div></div></div>`).join('')||'<div class="empty">No trades yet — the first one is placed at the next 1:30 lock.</div>'}
function quick(){if(!S)return;const wf=S.model.walk_forward||{},P=PERF;
 $('quick').innerHTML=`<div class="card stat"><div class="lbl">Live prediction accuracy</div><div class="v num">${P&&P.overall.n?pct(P.overall.accuracy):'—'}</div><div class="n">${P?P.overall.n+' officially resolved rounds'+(P.overall.n&&!P.overall.conclusive?' · small sample':''):'loading…'}</div></div>
 <div class="card stat"><div class="lbl">Backtest accuracy (walk-forward)</div><div class="v num">${pct(wf.accuracy)}</div><div class="n">${wf.n?wf.n.toLocaleString()+' out-of-sample rounds':'—'}${wf.ci95?' · 95% CI '+pct(wf.ci95[0])+'–'+pct(wf.ci95[1]):''}</div></div>
 <div class="card stat"><div class="lbl">Lock timing (mean)</div><div class="v num">${P&&P.timing&&P.timing.n?f2(P.timing.mean_ms,1)+' ms':'—'}</div><div class="n">${P&&P.timing&&P.timing.n?'max '+f2(P.timing.max_ms,1)+' ms after 1:30 · never early':'no locked rounds yet'}</div></div>`}
// ---------------------------------------------------------------- chart
function setup(cv){const dpr=window.devicePixelRatio||1,w=cv.clientWidth;if(!cv.dataset.h)cv.dataset.h=cv.getAttribute('height');const h=+cv.dataset.h;cv.width=w*dpr;cv.height=h*dpr;cv.style.height=h+'px';const x=cv.getContext('2d');x.scale(dpr,dpr);return [x,w,h]}
function drawChart(){const cv=$('chart');if(!C||!C.candles||!C.candles.length||!cv.clientWidth)return;const [x,w,h]=setup(cv),c=C.candles,n=c.length,pr=62,pb=22;
 const step=n>1?c[1][0]-c[0][0]:60,t0=c[0][0],t1=c[n-1][0]+step,X=t=>(t-t0)/(t1-t0)*(w-pr);
 const op=S&&S.round.open_price;let lo=Math.min(...c.map(b=>b[3])),hi=Math.max(...c.map(b=>b[2]));if(op){lo=Math.min(lo,op);hi=Math.max(hi,op)}const pad=(hi-lo)*.08||1;lo-=pad;hi+=pad;
 const Y=v=>(h-pb)-(v-lo)/(hi-lo)*(h-pb-6);x.font='10.5px '+getComputedStyle(document.body).fontFamily;
 for(let k=0;k<=4;k++){const v=lo+(hi-lo)*k/4,y=Y(v);x.strokeStyle='rgba(255,255,255,.05)';x.beginPath();x.moveTo(0,y);x.lineTo(w-pr,y);x.stroke();x.fillStyle='#6b7080';x.fillText(f2(v,0),w-pr+8,y+3)}
 const rs=C.round_start||(S&&S.round.start_ts);if(rs&&rs>=t0){x.fillStyle='rgba(124,140,255,.05)';x.fillRect(X(rs),0,Math.min(w-pr,X(rs+300))-X(rs),h-pb)}
 const cw=Math.max(1.5,(w-pr)/n*.62);
 c.forEach(b=>{const up=b[4]>=b[1],xc=X(b[0]+step/2);x.strokeStyle=x.fillStyle=up?'#22c58b':'#ff4d5e';x.beginPath();x.moveTo(xc,Y(b[2]));x.lineTo(xc,Y(b[3]));x.stroke();const y1=Y(Math.max(b[1],b[4])),y2=Y(Math.min(b[1],b[4]));x.fillRect(xc-cw/2,y1,cw,Math.max(1,y2-y1))});
 const e=(C.ema||{})['20'];if(e&&e.length===n){x.strokeStyle='#7c8cff';x.lineWidth=1.6;x.beginPath();e.forEach((v,i)=>{const xc=X(c[i][0]+step/2),y=Y(v);i?x.lineTo(xc,y):x.moveTo(xc,y)});x.stroke();x.lineWidth=1}
 if(op){x.setLineDash([5,4]);x.strokeStyle='#f5b544';x.beginPath();x.moveTo(0,Y(op));x.lineTo(w-pr,Y(op));x.stroke();x.setLineDash([]);x.fillStyle='#f5b544';x.fillRect(w-pr+2,Y(op)-8,pr-4,16);x.fillStyle='#0a0b0f';x.fillText(f2(op,0),w-pr+8,Y(op)+4)}
 if(rs&&rs+90>=t0&&rs+90<=t1){const xl=X(rs+90);x.strokeStyle='rgba(255,255,255,.7)';x.lineWidth=1.5;x.beginPath();x.moveTo(xl,0);x.lineTo(xl,h-pb);x.stroke();x.lineWidth=1;x.fillStyle='#fff';x.fillText('1:30 lock',Math.min(xl+5,w-pr-52),12)}
 const last=c[n-1][4];x.fillStyle=last>=(op||last)?'#22c58b':'#ff4d5e';x.fillRect(w-pr+2,Y(last)-8,pr-4,16);x.fillStyle='#fff';x.fillText(f2(last,0),w-pr+8,Y(last)+4);
 x.fillStyle='#6b7080';[0,Math.floor(n/2),n-1].forEach(i=>x.fillText(new Date(c[i][0]*1000).toLocaleTimeString('en-US',{timeZone:'America/New_York',hour:'numeric',minute:'2-digit',second:TF==='1s'?'2-digit':undefined}),Math.max(0,Math.min(X(c[i][0]),w-pr-56)),h-6));
 $('chsrc').textContent=(C.source||'')+' · '+(C.tf||TF);drawChart.m={X,Y,c,step,w,pr,t0,t1}}
$('chart').addEventListener('mousemove',ev=>{const m=drawChart.m;if(!m)return;const r=ev.target.getBoundingClientRect(),px=ev.clientX-r.left;if(px>m.w-m.pr){$('tip').style.display='none';return}
 const t=m.t0+px/(m.w-m.pr)*(m.t1-m.t0);let b=m.c.reduce((a,q)=>Math.abs(q[0]+m.step/2-t)<Math.abs(a[0]+m.step/2-t)?q:a);const tp=$('tip');tp.style.display='block';
 tp.innerHTML=`<b>${tET(b[0],TF==='1s')}</b><br><span class="num">O ${f2(b[1])} H ${f2(b[2])}<br>L ${f2(b[3])} C <span class="${b[4]>=b[1]?'up':'dn'}">${f2(b[4])}</span></span>`;tp.style.left=Math.min(px+14,r.width-150)+'px';tp.style.top='10px'});
$('chart').addEventListener('mouseleave',()=>$('tip').style.display='none');
function drawEq(){const cv=$('eq');if(!cv.clientWidth)return;const [x,w,h]=setup(cv);const E=EQ||{points:[],start:(T().demo_bankroll||0)};const p=[[0,E.start]].concat(E.points||[]);
 if(p.length<2){x.fillStyle='#6b7080';x.font='13px '+getComputedStyle(document.body).fontFamily;x.fillText('The equity curve appears after the first trade settles.',12,h/2);$('eqNote').textContent='';return}
 const vs=p.map(q=>q[1]);let lo=Math.min(...vs),hi=Math.max(...vs);const pad=(hi-lo)*.12||5;lo-=pad;hi+=pad;const pr=64,X=i=>i/(p.length-1)*(w-pr),Y=v=>h-14-(v-lo)/(hi-lo)*(h-24);
 for(let k=0;k<=3;k++){const v=lo+(hi-lo)*k/3;x.strokeStyle='rgba(255,255,255,.05)';x.beginPath();x.moveTo(0,Y(v));x.lineTo(w-pr,Y(v));x.stroke();x.fillStyle='#6b7080';x.font='10.5px sans-serif';x.fillText('$'+f2(v,0),w-pr+8,Y(v)+3)}
 const up=vs[vs.length-1]>=E.start,col=up?'34,197,139':'255,77,94';const g=x.createLinearGradient(0,0,0,h);g.addColorStop(0,`rgba(${col},.28)`);g.addColorStop(1,`rgba(${col},0)`);
 x.beginPath();p.forEach((q,i)=>i?x.lineTo(X(i),Y(q[1])):x.moveTo(X(i),Y(q[1])));x.lineTo(X(p.length-1),h);x.lineTo(0,h);x.fillStyle=g;x.fill();
 x.beginPath();p.forEach((q,i)=>i?x.lineTo(X(i),Y(q[1])):x.moveTo(X(i),Y(q[1])));x.strokeStyle=`rgb(${col})`;x.lineWidth=2;x.stroke();x.lineWidth=1;
 x.setLineDash([4,4]);x.strokeStyle='rgba(255,255,255,.25)';x.beginPath();x.moveTo(0,Y(E.start));x.lineTo(w-pr,Y(E.start));x.stroke();x.setLineDash([]);
 $('eqNote').textContent=`${E.mode||'demo'} · ${p.length-1} settled trades`}
// ---------------------------------------------------------------- trades
function trades(){if(!S)return;const tr=T(),lim=tr.limits||{},ro=!ITV();
 $('tstats').innerHTML=[[tr.mode==='live'?'Wallet balance':'Demo balance',usd(tr.balance),tr.mode==='demo'?'bankroll '+usd(tr.demo_bankroll,0):''],['Total P&L',`<span class="${cls(tr.realized_pnl)}">${sgn(tr.realized_pnl)}</span>`,'today '+sgn(tr.today_pnl)],
  ['Win rate',pct(tr.win_rate),`${tr.wins||0}W · ${tr.losses||0}L`],['ROI',`<span class="${cls(tr.roi)}">${tr.roi==null?'—':(tr.roi>0?'+':'')+pct(tr.roi)}</span>`,'on '+usd(tr.staked,0)+' staked'],['Max drawdown',usd(tr.max_drawdown),'open '+usd(tr.open_exposure)+(tr.fees?' · fees '+usd(tr.fees):'')]]
  .map(([l,v,n])=>`<div class="card stat"><div class="lbl">${l}</div><div class="v num">${v}</div><div class="n">${n}</div></div>`).join('');
 $('ctlRO').innerHTML=ro?'<div class="ro" style="margin-bottom:14px">View-only here. Change stake and limits on the private BTC-5 app.</div>':'';
 $('stakeQs').innerHTML=[5,10,20,50,100].map(v=>`<button data-v="${v}" class="${lim.stake_usd==v?'on':''}" ${ro?'disabled':''}>$${v}</button>`).join('');
 if(document.activeElement!==$('stakeIn'))$('stakeIn').value=lim.stake_usd??'';if(document.activeElement!==$('bankIn'))$('bankIn').value=tr.demo_bankroll??'';
 ['stakeIn','stakeSave','bankIn','resetDemo'].forEach(id=>$(id).disabled=ro);
 const L=(TR||[]).filter(t=>tfil==='all'||t.mode===tfil);
 const row=t=>`<tr><td>${dET(t.start_ts)}</td><td><span class="badge ${t.mode}">${t.mode}</span></td><td class="${dcl(t.side)}" style="font-weight:700">${t.side||'—'}</td><td class="num">${t.confidence||''}</td><td class="num">${t.cost!=null?usd(t.cost+(t.fee||0)):'—'}</td><td class="num">${t.entry_price?f2(t.entry_price*100,1)+'¢':'—'}</td><td class="num">${t.shares!=null?f2(t.shares,2):'—'}</td><td><span class="badge ${t.status}">${t.status}</span></td><td class="num ${cls(t.pnl)}">${t.pnl!=null?sgn(t.pnl):''}</td><td class="mut" style="white-space:normal;max-width:280px;font-size:12px">${esc((t.error||t.price_source||'').slice(0,120))}${t.order_id?'<br>order '+esc(t.order_id.slice(0,12))+'…':''}</td></tr>`;
 const card=t=>`<div class="tcard"><div style="min-width:0"><div style="font-weight:700"><span class="${dcl(t.side)}">${t.side||'—'}</span> · ${tET(t.start_ts,false)} <span class="badge ${t.mode}" style="margin-left:4px">${t.mode}</span></div><div class="lbl" style="margin-top:3px">${t.status==='SKIPPED'||t.status==='FAILED'?esc((t.error||'').slice(0,90)):usd((t.cost||0)+(t.fee||0))+' @ '+f2((t.entry_price||0)*100,1)+'¢ · '+f2(t.shares,1)+' sh'}</div></div><div style="text-align:right;flex:none"><span class="badge ${t.status}">${t.status}</span><div class="num ${cls(t.pnl)}" style="margin-top:4px;font-weight:700">${t.pnl!=null?sgn(t.pnl):''}</div></div></div>`;
 $('tlist').innerHTML=L.length?`<div class="tw ttable"><table><thead><tr><th>Round</th><th>Mode</th><th>Side</th><th>Conf.</th><th>Stake</th><th>Entry</th><th>Shares</th><th>Status</th><th>P&amp;L</th><th>Details</th></tr></thead><tbody>${L.map(row).join('')}</tbody></table></div><div class="tcards">${L.slice(0,60).map(card).join('')}</div>`:'<div class="empty">No trades yet.</div>';
 drawEq()}
$('stakeQs').onclick=e=>{const b=e.target.closest('[data-v]');if(b)saveTrading({stake_usd:+b.dataset.v})};
$('stakeSave').onclick=()=>saveTrading({stake_usd:+$('stakeIn').value});
$('autoT').onchange=e=>saveTrading({auto_trade:e.target.checked});$('autoT2').onchange=e=>saveTrading({auto_trade:e.target.checked});
$('resetDemo').onclick=()=>confirmBox('Reset demo balance?',`Start a fresh demo bankroll of <b>${usd(+$('bankIn').value)}</b>. Existing trades stay in history; P&amp;L stats restart.`,'Reset',async()=>{try{await post('/api/trading/demo/reset',{bankroll:+$('bankIn').value});toast('Demo balance reset');await refresh()}catch(e){toast(e.message,1)}});
async function saveTrading(body){if(!ITV())return;try{await post('/api/trading/settings',body);toast('Saved');await refresh()}catch(e){toast(e.message,1);await refresh()}}
async function refresh(){await tick();if(tab==='Trades')loadTrades();if(tab==='Settings')loadWallet()}
// ---------------------------------------------------------------- mode switch / modals
function modal(html){$('mbox').innerHTML=html;$('modal').classList.add('on')}
function closeModal(){$('modal').classList.remove('on')}
$('modal').onclick=e=>{if(e.target.id==='modal')closeModal()};
function confirmBox(title,body,btn,fn,red){modal(`<h2>${title}</h2><div class="sub">${body}</div><div class="row mt" style="justify-content:flex-end"><button class="btn sec" id="mNo">Cancel</button><button class="btn ${red?'red':''}" id="mYes">${btn}</button></div>`);$('mNo').onclick=closeModal;$('mYes').onclick=()=>{closeModal();fn()}}
function askToken(){return new Promise(res=>{modal(`<h2>Passcode required</h2><div class="sub">This BTC-5 server protects trading changes with an admin passcode (BTC5_ADMIN_TOKEN).</div><input class="inp mt" id="tokIn" type="password" placeholder="Passcode" autocomplete="off"><div class="row mt" style="justify-content:flex-end"><button class="btn sec" id="mNo">Cancel</button><button class="btn" id="mYes">Unlock</button></div>`);
 $('tokIn').focus();$('mNo').onclick=()=>{closeModal();res(false)};$('mYes').onclick=()=>{sessionStorage.btc5tok=$('tokIn').value;closeModal();res(true)}})}
$('modeSeg').onclick=async e=>{const b=e.target.closest('[data-m]');if(!b||!ITV()||!S)return;const tr=T();if(b.dataset.m===tr.mode)return;
 if(b.dataset.m==='demo'){try{await post('/api/trading/mode',{mode:'demo'});toast('Switched to DEMO — paper money');await refresh()}catch(e){toast(e.message,1)}return}
 let w=null;try{w=await j('/api/trading/wallet')}catch(e){}liveModal(w)};
function liveModal(w){const tr=T(),l=tr.limits||{};
 if(!w||!w.configured){modal(`<h2>Connect a wallet to go Live</h2><div class="sub">Live mode places real Polymarket orders with your own wallet. No wallet is configured on this server yet, so BTC-5 is staying in Demo.</div>
  <ol class="ev" style="margin-top:14px">${(w&&w.how_to_enable||['Set POLYMARKET_PRIVATE_KEY and POLYMARKET_FUNDER as environment variables (or HF Secrets) on the machine running BTC-5.','Restart BTC-5, then press LIVE again.']).map(x=>`<li>${esc(x)}</li>`).join('')}</ol>
  <div class="ro mt">Never paste a private key or seed phrase into a chat, a website form or this page. It goes only into the server's environment / private secrets.</div><div class="row mt" style="justify-content:flex-end"><button class="btn" id="mNo">Got it</button></div>`);$('mNo').onclick=closeModal;return}
 modal(`<h2 style="color:#ff8f9a">Switch to LIVE trading?</h2><div class="sub">BTC-5 will place <b>real-money</b> Polymarket orders${l.auto_trade?' automatically every round':''} using wallet <span class="num">${esc(w.funder||w.signer||'')}</span>. Predictions can be wrong — you can lose your stake every round.</div>
 <div class="lim"><div><span class="lbl">Stake per round</span><b class="num">${usd(l.stake_usd)}</b></div><div><span class="lbl">Max daily loss</span><b class="num">${usd(l.max_daily_loss_usd)}</b></div><div><span class="lbl">Max open exposure</span><b class="num">${usd(l.max_exposure_usd)}</b></div><div><span class="lbl">Max entry price</span><b class="num">${f2(l.max_entry_price*100,0)}¢</b></div></div>
 <div class="kv"><span>Wallet USDC</span><span class="num">${w.usdc_balance!=null?usd(w.usdc_balance):esc(w.balance_error||w.connect_error||'not read yet')}</span><span>Min confidence</span><span>${esc(l.min_confidence)}</span><span>Auto-trade</span><span>${l.auto_trade?'ON — every qualifying round':'OFF'}</span>${w.hard_stake_cap_usd?`<span>Hard stake cap</span><span class="num">${usd(w.hard_stake_cap_usd)}</span>`:''}</div>
 <label class="check"><input type="checkbox" id="ack"> I understand this uses real money and I accept these limits. I can press STOP at any time.</label>
 <div class="row" style="justify-content:flex-end"><button class="btn sec" id="mNo">Stay in Demo</button><button class="btn red" id="mYes" disabled>Enable LIVE</button></div>`);
 $('ack').onchange=e=>$('mYes').disabled=!e.target.checked;$('mNo').onclick=closeModal;
 $('mYes').onclick=async()=>{$('mYes').disabled=true;$('mYes').textContent='Connecting…';try{await post('/api/trading/mode',{mode:'live',confirm:true,limits:l});closeModal();toast('LIVE trading enabled');await refresh()}catch(e){closeModal();toast(e.message,1)}}}
$('estop').onclick=async()=>{try{const r=await post('/api/trading/emergency-stop',{});toast('STOPPED — auto-trade off, back to Demo'+(r.cancel_error?' (cancel error: '+r.cancel_error+')':''));await refresh()}catch(e){toast('Stop failed: '+e.message,1)}};
// ---------------------------------------------------------------- history
function twapNote(p){return p&&p.spot_disagrees?`<div class="note">Spot price closed ${p.spot_direction==='UP'?'higher':'lower'} than it opened, but Polymarket officially settled <b>${p.official_outcome}</b> on Chainlink's 60-second average price (TWAP). Graded against the official result.</div>`:''}
function histNote(r){const a=[];if(r.spot_disagrees)a.push(`spot ${r.spot_direction==='UP'?'up':'down'}, official ${r.official_outcome} (60s TWAP)`);if(r.regraded)a.push(r.regraded.from_outcome?`corrected from ${r.regraded.from_outcome} after Polymarket check`:'graded after Polymarket check');if(r.polymarket_url&&r.official_outcome)a.push(`<a href="${r.polymarket_url}" target="_blank" rel="noopener">check on Polymarket ↗</a>`);return a.join(' · ')}
function history(){const P=PERF,H=HIST;if(!P||!H){$('hist').innerHTML='<div class="empty">Loading…</div>';return}
 const st=(l,a)=>`<div class="card stat"><div class="lbl">${l}</div><div class="v num">${a&&a.n?pct(a.accuracy):'—'}</div><div class="n">${a?a.correct+' of '+a.n+' correct':'—'}</div></div>`;
 $('hstats').innerHTML=st('All resolved',P.overall)+st('Last 10',P.last['10'])+st('Last 50',P.last['50'])+st('High confidence',P.high_conf)+st('Low confidence',P.low_conf);
 $('hn').textContent=`${P.total_recorded} recorded · ${P.resolved_predictions} resolved · ${P.pending} pending · ${P.missed} missed`;
 const rows=H.map(r=>`<tr><td>${dET(r.start_ts)}</td><td class="${dcl(r.prediction)}" style="font-weight:700">${r.status==='MISSED'?'<span class="mut">—</span>':r.prediction||'—'}</td><td class="num">${r.p_up!=null?pct(r.prediction==='UP'?r.p_up:r.p_down):''}</td><td>${esc(r.confidence||'')}</td><td class="num">${r.deviation_ms!=null&&r.status!=='MISSED'?'+'+f2(r.deviation_ms,1)+' ms':''}</td><td class="${dcl(r.official_outcome)}">${r.official_outcome||'<span class="mut">—</span>'}</td><td><span class="badge ${r.result}">${r.result||r.status}</span></td><td class="mut" style="white-space:normal;font-size:12px;max-width:320px">${esc(r.status==='MISSED'?r.missed_reason:r.status!=='LOCKED'?r.status:'')}${r.status==='LOCKED'?histNote(r):''}</td></tr>`).join('');
 const cards=H.slice(0,80).map(r=>`<div class="tcard"><div><div style="font-weight:700">${tET(r.start_ts,false)} · <span class="${dcl(r.prediction)}">${r.status==='MISSED'?'missed':r.prediction}</span></div><div class="lbl">${r.status==='MISSED'?esc((r.missed_reason||'').slice(0,70)):esc(r.confidence||'')+' · result '+(r.official_outcome||'pending')}</div>${r.status==='LOCKED'?'<div class="lbl">'+histNote(r)+'</div>':''}</div><span class="badge ${r.result}">${r.result||r.status}</span></div>`).join('');
 $('hist').innerHTML=`<div class="tw ttable"><table><thead><tr><th>Round</th><th>Call</th><th>Prob.</th><th>Confidence</th><th>Lock timing</th><th>Official</th><th>Result</th><th>Notes</th></tr></thead><tbody>${rows}</tbody></table></div><div class="tcards">${cards}</div>`}
// ---------------------------------------------------------------- model & agents
function model(){if(!S)return;const r=S.round,ag=r.agents||{},rel=S.model.agent_reliability||{},v=ag.VALIDATOR||{},M=S.model;
 $('valcard').innerHTML=`<h3>Agent 6 · Final validator <small>${tET(r.start_ts,false)} round</small></h3>${v.direction?`<div class="dir ${dcl(v.direction)}" style="font-size:52px">${arrow(v.direction)}${v.direction}</div><div class="sub mt8">${pct(v.direction==='UP'?v.p_up:v.p_down)} · ${esc(v.confidence)} · agreement ${esc(v.agreement)}</div><ul class="ev">${(v.checks||[]).concat(v.evidence||[]).slice(0,8).map(e=>`<li>${esc(e)}</li>`).join('')}</ul>`:'<div class="sub">The five specialists are analysing this round. Their directions are hidden until the 1:30 lock, then frozen permanently.</div>'}`;
 const wf=M.walk_forward||{};$('modelcard').innerHTML=`<h3>Champion model</h3><div class="kv"><span>Version</span><span class="num">${esc(M.version||'fallback')}</span><span>Architecture</span><span>${esc(M.arch||'—')}</span><span>Walk-forward accuracy</span><span class="num">${pct(wf.accuracy)} (n=${wf.n||'—'})</span><span>Brier · ECE</span><span class="num">${f2(wf.brier,3)} · ${f2(wf.ece,3)}</span><span>Challenger</span><span>${esc(((MODELS||{}).challenger_status||{}).decision||'—')}</span><span>Retraining</span><span>every ${(S.settings.model||{}).retrain_every||12} resolved rounds on real data</span></div>
 ${ITV()?`<div class="row mt"><button class="btn sec" id="bRetrain">Retrain now</button><button class="btn sec" id="bBT">Run backtest</button></div>`:''}`;
 if(ITV()){$('bRetrain').onclick=async()=>{try{await post('/api/model/retrain');toast('Retrain started')}catch(e){toast(e.message,1)}};$('bBT').onclick=async()=>{try{await post('/api/backtest/run',{days:(S.settings.backtest||{}).days||10});toast('Backtest started')}catch(e){toast(e.message,1)}}}
 $('agents').innerHTML=['TREND','STRUCTURE','ORDERFLOW','VOLATILITY','DERIVS'].map(k=>{const a=ag[k]||{name:k},rl=rel[k];return `<div class="agent"><div class="row" style="justify-content:space-between"><div class="nm">${esc(a.name||k)}</div><span class="badge ${dcl(a.direction)}">${esc(a.status||'')}</span></div><div class="d ${dcl(a.direction)}">${a.direction||'—'}</div>${a.p_up!=null?`<div class="mbar"><i style="width:${a.p_up*100}%"></i></div><div class="lbl">P(UP) ${f2(a.p_up,3)} · ${esc(a.confidence||'')}</div>`:`<div class="lbl">${esc(a.note||'Waiting for 1:30')}</div>`}<div class="lbl mt8">Backtest reliability ${rl?pct(rl.accuracy)+' · n='+rl.n:'—'}</div>${(a.evidence||[]).length?`<ul class="ev">${a.evidence.slice(0,3).map(e=>`<li>${esc(e)}</li>`).join('')}</ul>`:''}</div>`}).join('');
 const B=BT;if(B&&B.models){const ch=B.models[B.champion_arch]||B.models.stack||{},vs=B.vs_best_baseline||{};
  $('btcard').innerHTML=`<h3>Walk-forward backtest <small>${B.n_test} out-of-sample rounds</small></h3><div class="big num">${pct(ch.accuracy)}</div><div class="sub">${dET(B.first_round)} → ${dET(B.last_round)}</div>
  <div class="note">${vs.baseline?(vs.significant?`Beats the best simple baseline (${esc(vs.baseline.replace(/_/g,' '))}) by ${pct(vs.diff_accuracy)} — statistically significant.`:`Edge of ${pct(vs.diff_accuracy)} over the best simple baseline (${esc(vs.baseline.replace(/_/g,' '))}) is not statistically significant.`):''}</div>
  <div class="tw mt"><table><thead><tr><th>Confidence</th><th>Rounds</th><th>Accuracy</th></tr></thead><tbody>${CONF.slice().reverse().filter(k=>ch.by_confidence&&ch.by_confidence[k]).map(k=>`<tr><td>${k}</td><td class="num">${ch.by_confidence[k].n}</td><td class="num">${pct(ch.by_confidence[k].accuracy)}</td></tr>`).join('')}</tbody></table></div>`}
 else $('btcard').innerHTML='<h3>Walk-forward backtest</h3><div class="empty">No backtest yet.</div>';
 const i=S.intel||{};$('intel').innerHTML=`<h3>Market intelligence</h3><div class="kv"><span>Regime</span><span>${esc(i.regime||'—')}</span><span>RSI · ADX</span><span class="num">${f2(i.rsi,1)} · ${f2(i.adx,1)}</span><span>ATR (1m)</span><span class="num">${usd(i.atr)}</span><span>Volatility percentile</span><span class="num">${i.vol_percentile==null?'—':pct(i.vol_percentile,0)}</span><span>Taker buy share</span><span class="num">${i.taker_buy_share==null?'—':pct(i.taker_buy_share)}</span><span>Data quality</span><span class="num">${f2(i.data_quality,2)}</span></div>
 <div class="tw mt"><table><thead><tr><th>Exchange</th><th>Price</th><th>vs Chainlink</th><th>Age</th></tr></thead><tbody>${Object.entries(i.exchanges||{}).map(([k,q])=>`<tr><td>${k}</td><td class="num">${f2(q.price)}</td><td class="num">${q.vs_chainlink_usd==null?'—':(q.vs_chainlink_usd>=0?'+':'')+f2(q.vs_chainlink_usd)}</td><td class="num ${q.fresh?'':'dn'}">${q.age_s==null?'—':q.age_s+'s'}</td></tr>`).join('')}</tbody></table></div>`}
// ---------------------------------------------------------------- settings
function wallet(){const w=WAL,tr=T();
 if(!LOCAL||!w){$('wallet').innerHTML=`<h3>Wallet</h3><div class="row"><span class="badge ${tr.live&&tr.live.configured?'up':''}">${tr.live&&tr.live.configured?'CONFIGURED':'NOT CONFIGURED'}</span><span class="badge ${tr.mode}">${tr.mode.toUpperCase()}</span></div><div class="note">Wallet details are only shown on the private BTC-5 app.</div>`;return}
 $('wallet').innerHTML=`<h3>Polymarket wallet <span class="badge ${w.connected?'up':w.configured?'SKIPPED':''}">${w.connected?'CONNECTED':w.configured?'CONFIGURED':'NOT CONNECTED'}</span></h3>
 ${w.configured?`<div class="kv"><span>Funder wallet</span><span class="num">${esc(w.funder||'—')}</span><span>Signer</span><span class="num">${esc(w.signer||'—')}</span><span>Signature type</span><span class="num">${w.signature_type}</span><span>USDC balance</span><span class="num">${w.usdc_balance!=null?usd(w.usdc_balance):'—'}</span><span>USDC allowance</span><span class="num">${w.usdc_allowance!=null?usd(w.usdc_allowance):'—'}</span>${w.hard_stake_cap_usd?`<span>Hard stake cap</span><span class="num">${usd(w.hard_stake_cap_usd)}</span>`:''}</div>${w.connect_error||w.balance_error?`<div class="note wn">${esc(w.connect_error||w.balance_error)}</div>`:''}`
 :`<div class="sub">Live mode is off until you add your wallet to this server's environment. BTC-5 keeps trading with demo money until then.</div><ol class="ev" style="margin-top:12px">${(w.how_to_enable||[]).map(x=>`<li>${esc(x)}</li>`).join('')}</ol>`}
 <div class="ro mt">🔒 Keys are read only from environment variables / HF Secrets. They are never stored in the database, shown on this page, logged, or requested in chat.</div>
 <div class="kv mt"><span>Passcode protection</span><span>${w.admin_token_required?'ON (BTC5_ADMIN_TOKEN)':'off'}</span><span>Boot mode</span><span>Always DEMO after restart</span></div>`}
function settings(force){const tr=T(),l=tr.limits||{},ro=!ITV(),h=S.health;
 $('limMode').innerHTML=`<span class="badge ${tr.mode}">${tr.mode.toUpperCase()}</span>`;$('riskRO').innerHTML=ro?'<div class="ro" style="margin-bottom:14px">View-only here.</div>':'';
 const set=(id,v)=>{const e=$(id);if(document.activeElement!==e&&(force||!e.dataset.dirty))e.value=v;e.disabled=ro};
 set('l_stake',l.stake_usd);set('l_dl',l.max_daily_loss_usd);set('l_ex',l.max_exposure_usd);set('l_px',Math.round((l.max_entry_price||0)*100));set('l_conf',l.min_confidence);$('limSave').disabled=ro;
 if(force||!settings.t||Date.now()-settings.t>5000){settings.t=Date.now();wallet();
  const st=S.settings;$('engineCard').innerHTML=`<h3>Prediction engine</h3><div class="kv"><span>Engine mode</span><span>${ITV()?`<select class="inp" id="e_mode" style="height:34px;width:auto;font-size:13px"><option value="automatic">Automatic (lock every round)</option><option value="monitoring">Monitoring only</option></select>`:esc(st.mode)}</span><span>Agent weighting</span><span>${esc((st.weighting||{}).mode)}</span><span>Retrain every</span><span>${(st.model||{}).retrain_every} resolved rounds</span><span>Market auto-detect</span><span>${st.auto_detect?'on':'off'}</span></div>`;
  if(ITV()){$('e_mode').value=st.mode;$('e_mode').onchange=async e=>{try{await post('/api/settings',{mode:e.target.value});toast('Engine mode saved')}catch(x){toast(x.message,1)}}}
  $('healthCard').innerHTML=`<h3>System health <span class="badge ${h.ok?'up':'dn'}">${h.ok?'HEALTHY':'DEGRADED'}</span></h3><div class="kv"><span>Clock vs exchanges</span><span class="num">${h.clock.offset_ms} ms · ${h.clock.synced?'synced':'NOT synced'}</span><span>Event-loop lag p50/max</span><span class="num">${f2(h.loop_lag_ms.p50,1)} / ${f2(h.loop_lag_ms.max,1)} ms</span><span>Uptime</span><span class="num">${Math.floor(h.uptime_s/3600)}h ${Math.floor(h.uptime_s%3600/60)}m</span><span>Training</span><span>${h.training.running?'running…':h.training.error?'<span class="dn">'+esc(h.training.error)+'</span>':'idle'}</span><span>Database</span><span class="num">${f2(h.db_bytes/1e6,1)} MB</span><span>Data view</span><span>${LOCAL?'live server':'feed '+Math.round(feedAge||0)+'s old'}</span></div>`;
  $('feeds').innerHTML=Object.values(h.feeds).map(f=>`<span class="chip"><span class="d ${f.status==='LIVE'?'ok':f.status==='DISABLED'?'':'bad'}" style="width:7px;height:7px;border-radius:50%;display:inline-block"></span>${esc(f.name)} <span class="mut num">${f.age_s==null?'':f.age_s+'s'}</span></span>`).join('');
  $('logs').innerHTML=(S.logs||[]).slice().reverse().map(x=>`<div class="${x.level}"><span class="mut">${tET(x.ts)}</span> ${esc(x.source)} · ${esc(x.message)}</div>`).join('')}}
['l_stake','l_dl','l_ex','l_px','l_conf'].forEach(id=>$(id).addEventListener('input',e=>e.target.dataset.dirty=1));
$('limSave').onclick=async()=>{const b={stake_usd:+$('l_stake').value,max_daily_loss_usd:+$('l_dl').value,max_exposure_usd:+$('l_ex').value,max_entry_price:+$('l_px').value/100,min_confidence:$('l_conf').value};
 try{await post('/api/trading/settings',b);['l_stake','l_dl','l_ex','l_px','l_conf'].forEach(id=>delete $(id).dataset.dirty);toast('Risk limits saved');await refresh();settings(true)}catch(e){toast(e.message,1)}};
// ---------------------------------------------------------------- boot
window.addEventListener('resize',()=>{if(tab==='Live')drawChart();if(tab==='Trades')drawEq()});
show(tab);tick();setInterval(tick,LOCAL?1000:4000);setInterval(()=>{if(S&&tab==='Live')live()},250);
if(LOCAL){setInterval(()=>{if(tab==='Live')loadCandles()},5000);setInterval(()=>{if(tab==='Trades')loadTrades()},5000);setInterval(()=>{if(tab==='History'||tab==='Live')loadHist()},15000);setInterval(()=>{if(tab==='Settings')loadWallet()},15000);setInterval(()=>{if(tab==='Model')model()},3000);loadHist()}
else setInterval(()=>{if(tab==='Trades')trades();if(tab==='History')history();if(tab==='Model')model()},4000);
