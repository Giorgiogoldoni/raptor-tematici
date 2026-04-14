#!/usr/bin/env python3
"""
🦅 RAPTOR TEMATICI — Autonomous Thematic ETF Portfolio Manager
13 thematic groups, 3 positions max per group
KAMA veloce (n=5) + ER + Vortex + RVI
Target: +7% | Time stop: 8 days | Trailing: 1.5xATR
Runs hourly 09:00-19:00 CET Mon-Fri via GitHub Actions
"""

import json, os
from datetime import datetime, date
import pytz
import yfinance as yf
import pandas as pd

ROME_TZ    = pytz.timezone("Europe/Rome")
MAX_POS    = 3
KAMA_N     = 5
KAMA_FAST  = 2
KAMA_SLOW  = 20
TARGET_PCT = 7.0
TRAIL_MULT = 1.5
TIME_STOP  = 8
PRE_ALERT  = 5

GROUPS = {
    "ai_tech": {
        "name":"🤖 AI & TECH","color":"#0969da",
        "tickers":{
            "AIAA.MI":"WisdomTree AI & Tech ETF","AIAI.MI":"L&G Artificial Intelligence ETF",
            "AI4UJ.MI":"iShares Automation & Robotics","AINF.MI":"WisdomTree AI Infrastructure",
            "AIQE.MI":"iShares AI Equity ETF","GOAI.MI":"Invesco AI ETF",
            "WTAI.MI":"WisdomTree AI ETF","JEDI.MI":"Amundi MSCI AI ETF",
            "XSGI.MI":"Xtrackers AI & Big Data ETF","GDIG.MI":"iShares Digital Security ETF",
            "DGTL.MI":"Rize Digital Economy ETF","CLOU.MI":"WisdomTree Cloud Computing ETF",
            "CTEK.MI":"Amundi PEA Digital Economy","XCTE.MI":"Xtrackers Cybersecurity ETF",
            "XDER.MI":"Xtrackers Digital Enablers ETF","FAMMAI.MI":"First Asset MSCI AI ETF",
            "FAMAMW.MI":"First Asset MSCI All World","FAMMWF.MI":"First Asset MSCI World Fin",
            "FAMMWS.MI":"First Asset MSCI World Sust","FAMTEL.MI":"First Asset MSCI Telecom",
            "FAMWCS.MI":"First Asset MSCI World CS",
        }
    },
    "difesa": {
        "name":"🛡️ DIFESA & SICUREZZA","color":"#cf222e",
        "tickers":{
            "ARMI.MI":"VanEck Defense ETF","ARMR.MI":"Amundi Future of Defence ETF",
            "DFND.MI":"Rize Defence Innovation ETF","DFNS.MI":"HANetf Future of Defence ETF",
            "WDEF.MI":"WisdomTree Defence ETF","LOCK.MI":"iShares Aerospace & Defence ETF",
            "BUG.MI":"Global X Cybersecurity ETF","CYBO.MI":"Ossiam Cyber Security ETF",
            "WCBR.MI":"WisdomTree Cybersecurity ETF","VPN.MI":"Global X Data Center ETF",
            "ICBR.MI":"iShares Digital Security ETF","ISPY.MI":"iShares Core S&P 500 ETF",
        }
    },
    "energia": {
        "name":"⚡ ENERGIA & RINNOVABILI","color":"#e3b341",
        "tickers":{
            "RENW.MI":"HANetf S&P Global Clean Energy","RNRG.MI":"Rize Sustainable Future Energy",
            "REUS.MI":"Amundi MSCI New Energy ETF","SOLR.MI":"Global X Solar Energy ETF",
            "HDRO.MI":"HANetf Hydrogen Economy ETF","HTWO.MI":"L&G Hydrogen Economy ETF",
            "HYGN.MI":"VanEck Hydrogen Economy ETF","VOLT.MI":"Amundi MSCI New Energy Transition",
            "ELCR.MI":"iShares Global Clean Energy ETF","WNDE.MI":"WisdomTree New Energy ETF",
            "WNDY.MI":"WisdomTree Wind Energy ETF","WRNW.MI":"WisdomTree Renewable Energy ETF",
            "NRJC.MI":"Amundi Energy Transition ETF","EEA.MI":"iShares MSCI Europe ESG ETF",
            "GCLE.MI":"iShares Global Clean Energy UCITS","NCLR.MI":"VanEck Uranium & Nuclear ETF",
            "NUCL.MI":"Sprott Uranium Miners ETF","U3O8.MI":"Global X Uranium ETF",
            "URNJ.MI":"Sprott Junior Uranium Miners ETF","OIH.MI":"VanEck Oil Services ETF",
            "XNGI.MI":"Xtrackers MSCI World Energy ETF","ICLN":"iShares Global Clean Energy USD",
        }
    },
    "salute": {
        "name":"💊 SALUTE & BIOTECH","color":"#1a7f37",
        "tickers":{
            "HEAL.MI":"iShares Healthcare Innovation ETF","BIOT.MI":"iShares Nasdaq Biotechnology",
            "CURE.MI":"Global X Genomics & Biotechnology","DOCT.MI":"Amundi MSCI Healthcare ETF",
            "GNOM.MI":"iShares Genomics Immunology ETF","WDNA.MI":"WisdomTree Biorevolution ETF",
            "GENDEE.MI":"Rize Medical Technology ETF","EDOC.MI":"Global X Telemedicine ETF",
            "AGED.MI":"iShares Ageing Population ETF","XGEN.MI":"Xtrackers MSCI World Health",
            "XNNV.MI":"Xtrackers Artificial Intelligence","LABL.MI":"WisdomTree Food Innovation ETF",
        }
    },
    "crypto": {
        "name":"🔗 BLOCKCHAIN & CRYPTO","color":"#f0883e",
        "tickers":{
            "BKCH.MI":"Global X Blockchain ETF","BTC.MI":"ETC Group Physical Bitcoin",
            "DAPP.MI":"VanEck Crypto & Blockchain Innovators","WEB3.MI":"Bitwise Web3 ETF",
            "BTECH.MI":"iShares Blockchain Technology ETF","BTECJ.MI":"iShares Blockchain JPY Hedged",
            "BLTH.MI":"Invesco CoinShares Blockchain ETF",
        }
    },
    "mobilita": {
        "name":"🚗 MOBILITA & EV","color":"#bc4c00",
        "tickers":{
            "ECAR.MI":"iShares Electric Vehicles ETF","EMOVE.MI":"Amundi Future Mobility ETF",
            "EMOVJ.MI":"Amundi Future Mobility JPY Hedged","DRVE.MI":"Rize Electric Vehicle ETF",
            "AUCO.MI":"L&G Eaton Vance Automation ETF","BATT.MI":"Global X Lithium & Battery Tech",
            "LITU.MI":"WisdomTree Battery Solutions ETF","LITM.MI":"iShares Lithium & Battery ETF",
        }
    },
    "infrastrutture": {
        "name":"🏗️ INFRASTRUTTURE & REAL ESTATE","color":"#57606a",
        "tickers":{
            "INFR.MI":"iShares Global Infrastructure ETF","PAVE.MI":"Global X US Infrastructure ETF",
            "EPRA.MI":"iShares Developed Markets Property Yield","EPRE.MI":"SPDR Global Real Estate ETF",
            "SCITY.MI":"iShares Smart City Infrastructure","CITY.MI":"Amundi Smart City ETF",
            "CIT.MI":"Global X Smart City ETF","CITE.MI":"iShares Digitalisation ETF",
            "XLPE.MI":"SPDR S&P US Consumer Discretionary","XRES.MI":"Xtrackers MSCI World Real Estate",
        }
    },
    "risorse": {
        "name":"🌾 RISORSE & MATERIE PRIME","color":"#7d4e00",
        "tickers":{
            "COPM.MI":"iShares Copper Producers ETF","COPR.MI":"Global X Copper Miners ETF",
            "COPX.MI":"Global X Copper ETF","CROP.MI":"iShares Agribusiness ETF",
            "GDX.MI":"VanEck Gold Miners ETF","GDXJ.MI":"VanEck Junior Gold Miners ETF",
            "REMX.MI":"VanEck Rare Earth & Strategic Metals","RARE.MI":"VanEck Rare Earth ETF",
            "METL.MI":"WisdomTree Industrial Metals ETF","METAA.MI":"Amundi MSCI Metals & Mining",
            "METAJ.MI":"Amundi MSCI Metals & Mining JPY","METE.MI":"iShares MSCI Global Metals",
            "SILV.MI":"iShares Silver ETF","WSLV.MI":"WisdomTree Physical Silver ETF",
            "VEGI.MI":"iShares Agribusiness ETF EUR","FOFD.MI":"Rize Food Revolution ETF",
            "GSM.MI":"Global X Silver Miners ETF",
        }
    },
    "digitale": {
        "name":"📱 DIGITALE & ECOMMERCE","color":"#6e40c9",
        "tickers":{
            "EBIZ.MI":"Global X E-commerce ETF","EBUY.MI":"Amundi MSCI E-commerce ETF",
            "ECOM.MI":"VanEck Retail ETF","DPAY.MI":"Amundi Digital Economy ETF",
            "FINX.MI":"Global X FinTech ETF","INQQ.MI":"iShares EM Digital Economy ETF",
            "SNSR.MI":"Global X Internet of Things ETF","XFNT.MI":"Xtrackers FinTech Innovation",
            "XMOV.MI":"Xtrackers Future Mobility ETF","XDG3.MI":"Xtrackers Digital Disruption",
            "XDG6.MI":"Xtrackers Digital Infrastructure","XDG7.MI":"Xtrackers Digital Payments",
            "XDGI.MI":"Xtrackers Digital Globalisation",
        }
    },
    "media": {
        "name":"🎮 MEDIA & ENTERTAINMENT","color":"#0550ae",
        "tickers":{
            "ESPO.MI":"VanEck Video Gaming & Esports ETF","ESPY.MI":"iShares eSports ETF",
            "ESGO.MI":"Global X Video Games & Esports ETF","MCHT.MI":"iShares Metaverse ETF",
            "MTVS.MI":"WisdomTree Metaverse ETF","MTAV.MI":"Roundhill Ball Metaverse ETF",
            "GLUX.MI":"Amundi S&P Global Luxury ETF","BRIJ.MI":"iShares MSCI India ETF",
            "HNSC.MI":"iShares Hang Seng Tech ETF","XS8R.MI":"Xtrackers S&P 500 Info Tech",
            "XG11.MI":"Xtrackers MSCI World Growth ETF","XG12.MI":"Xtrackers MSCI World Value ETF",
        }
    },
    "acqua": {
        "name":"🌊 ACQUA & AMBIENTE","color":"#0969da",
        "tickers":{
            "AQWA.MI":"iShares Global Water ETF","WATC.MI":"Lyxor World Water ETF",
            "WTRE.MI":"WisdomTree Water ETF","GLUG.MI":"iShares Global Water UCITS ETF",
            "HYDE.MI":"VanEck Circular Economy ETF","SDG9.MI":"iShares MSCI Global Sustainable",
            "WGRO.MI":"WisdomTree Global Growth ETF","ROBO.MI":"Robo Global Robotics ETF",
            "BOTZ.MI":"Global X Robotics & AI ETF",
        }
    },
    "semiconduttori": {
        "name":"💾 SEMICONDUTTORI","color":"#6e40c9",
        "tickers":{
            "SMH.MI":"VanEck Semiconductor ETF","SEME.MI":"iShares MSCI Global Semiconductors",
            "CHIP.MI":"Amundi PHLX Semiconductor ETF","FCHP.DE":"Franklin FTSE Semiconductor ETF",
            "USTEC.MI":"iShares NASDAQ 100 UCITS ETF",
        }
    },
    "altro": {
        "name":"🏥 ALTRO TEMATICO","color":"#57606a",
        "tickers":{
            "HERU.MI":"HANetf European Equity ETF","DMAT.MI":"iShares Diversified Materials ETF",
            "ROE.MI":"iShares Return on Equity ETF","UNIC.MI":"iShares Unicorns ETF",
            "QNTM.MI":"Defiance Quantum ETF","QUAD.MI":"Invesco Quantitative ETF",
            "RAYZ.MI":"Rize Sustainable Future Food ETF","MILL.MI":"Global X Millennials ETF",
            "MLPS.MI":"Global X MLP ETF","WMGT.MI":"WisdomTree Quality Dividend ETF",
            "WBLK.MI":"WisdomTree Blockchain ETF","ISAG.MI":"iShares Ageing Population ETF",
            "KWBE.MI":"L&G ROBO Global Robotics ETF","IVAI.MI":"Invesco AI ETF",
            "IVDF.DE":"Invesco Defence Innovation ETF","CAUT.MI":"iShares Automation & Robotics ETF",
        }
    },
}

XEON_TICKER = "XEON.MI"

def ema_arr(values, period):
    k=2/(period+1); r=[values[0]]
    for v in values[1:]: r.append(v*k+r[-1]*(1-k))
    return r

def calc_kama(prices, n=KAMA_N, fast=KAMA_FAST, slow=KAMA_SLOW):
    fsc=2/(fast+1); ssc=2/(slow+1); result=list(prices[:n])
    for i in range(n,len(prices)):
        d=abs(prices[i]-prices[i-n])
        v=sum(abs(prices[j]-prices[j-1]) for j in range(i-n+1,i+1))
        er=d/v if v>0 else 0; sc=(er*(fsc-ssc)+ssc)**2
        result.append(result[-1]+sc*(prices[i]-result[-1]))
    return result

def calc_er(prices, n=10):
    if len(prices)<n+1: return 0.0
    d=abs(prices[-1]-prices[-n-1])
    v=sum(abs(prices[i]-prices[i-1]) for i in range(-n,0))
    return round(d/v if v>0 else 0,4)

def calc_ao_baffetti(high, low):
    mid=[(h+l)/2 for h,l in zip(high,low)]
    if len(mid)<34: return 0.0,0
    series=[sum(mid[i-4:i+1])/5-sum(mid[i-33:i+1])/34 for i in range(33,len(mid))]
    baff=0
    for i in range(len(series)-1,0,-1):
        if series[i]>series[i-1]: baff+=1
        else: break
    return round(series[-1],6),baff

def calc_rsi(prices, n=14):
    if len(prices)<n+1: return 50.0
    d=[prices[i]-prices[i-1] for i in range(1,len(prices))]
    g=[max(x,0) for x in d]; l=[max(-x,0) for x in d]
    ag,al=sum(g[:n])/n,sum(l[:n])/n
    for i in range(n,len(g)): ag=(ag*(n-1)+g[i])/n; al=(al*(n-1)+l[i])/n
    return round(100-100/(1+ag/al) if al>0 else 100,1)

def calc_atr(high, low, close, n=14):
    if len(close)<2: return 0.0
    tr=[max(high[i]-low[i],abs(high[i]-close[i-1]),abs(low[i]-close[i-1])) for i in range(1,len(close))]
    av=sum(tr[:n])/min(n,len(tr))
    for i in range(min(n,len(tr)),len(tr)): av=(av*(n-1)+tr[i])/n
    return round(av,5)

def calc_sar(high, low, af0=0.02, af_max=0.2):
    if len(high)<5: return low[-1],True
    sar,ep,af,bull=low[0],high[0],af0,True; sars=[sar]
    for i in range(1,len(high)):
        prev=sars[-1]
        if bull:
            new=prev+af*(ep-prev)
            cands=low[max(0,i-2):i]; new=min(new,min(cands)) if cands else new
            if low[i]<new: bull,new,ep,af=False,ep,low[i],af0
            else:
                if high[i]>ep: ep=high[i]; af=min(af+af0,af_max)
        else:
            new=prev+af*(ep-prev)
            cands=high[max(0,i-2):i]; new=max(new,max(cands)) if cands else new
            if high[i]>new: bull,new,ep,af=True,ep,high[i],af0
            else:
                if low[i]<ep: ep=low[i]; af=min(af+af0,af_max)
        sars.append(new)
    return round(sars[-1],5),bull

def trendycator(close):
    if len(close)<55: return "GRIGIO"
    e21=ema_arr(close,21); e55=ema_arr(close,55)
    if close[-1]>e21[-1]>e55[-1]: return "VERDE"
    if close[-1]<e21[-1]<e55[-1]: return "ROSSO"
    return "GRIGIO"

def calc_vortex(high, low, close, n=14):
    if len(close)<n+1: return 1.0,1.0,False
    vm_p=[abs(high[i]-low[i-1]) for i in range(1,len(close))]
    vm_m=[abs(low[i]-high[i-1]) for i in range(1,len(close))]
    tr=[max(high[i]-low[i],abs(high[i]-close[i-1]),abs(low[i]-close[i-1])) for i in range(1,len(close))]
    ts=sum(tr[-n:]); vip=round(sum(vm_p[-n:])/ts if ts>0 else 1,4); vim=round(sum(vm_m[-n:])/ts if ts>0 else 1,4)
    return vip,vim,vip>vim

def calc_rvi(close, open_, high, low, n=10):
    if len(close)<n+4: return 0.0,0.0,False
    num,den=[],[]
    for i in range(3,len(close)):
        nv=(close[i]-open_[i]+2*(close[i-1]-open_[i-1])+2*(close[i-2]-open_[i-2])+(close[i-3]-open_[i-3]))/6
        dv=(high[i]-low[i]+2*(high[i-1]-low[i-1])+2*(high[i-2]-low[i-2])+(high[i-3]-low[i-3]))/6
        num.append(nv); den.append(dv)
    if len(num)<n: return 0.0,0.0,False
    rs=[sum(num[i-n+1:i+1])/(sum(den[i-n+1:i+1]) or 1) for i in range(n-1,len(num))]
    if len(rs)<4: return 0.0,0.0,False
    ss=[(rs[i]+2*rs[i-1]+2*rs[i-2]+rs[i-3])/6 for i in range(3,len(rs))]
    if not ss: return 0.0,0.0,False
    return round(rs[-1],6),round(ss[-1],6),rs[-1]>ss[-1]

def calc_score(er, baff, k_pct, p7, p30, ao_pos, cross, trend, vortex_bull=False, rvi_bull=False):
    s=(er*18+min(baff,5)*2+min(abs(k_pct),5)*1+max(-5,min(5,p7))*2
       +max(-10,min(10,p30))*0.5+(3 if ao_pos else 0)
       +(8 if cross<=3 else 5 if cross<=10 else 2 if cross<=20 else 0))
    if vortex_bull: s+=2
    if rvi_bull: s+=2
    if not vortex_bull and not rvi_bull: s-=4
    if trend=="ROSSO": s*=0.6
    return round(max(0,min(75,s)),1)

def calc_signal(price, kama_v, er, baff, trend, ao, sar_bull, vortex_bull, rvi_bull):
    if price<kama_v and trend=="ROSSO": return "STOP"
    if price<kama_v: return "USCITA"
    if ao<=0 and trend=="GRIGIO": return "ATTENZIONE"
    n=sum([price>kama_v, trend=="VERDE", er>=0.40, baff>=2,
           sar_bull, ao>0, vortex_bull, rvi_bull])
    if n>=6: return "LONG_FORTE"
    if n>=4: return "LONG"
    if n>=3: return "EARLY"
    if n>=1: return "WATCH"
    return "ATTENZIONE"

def qualifies(signal, score, price, kama_v):
    if price<=kama_v: return False
    return score>={"LONG_FORTE":38,"LONG":32,"EARLY":26,"WATCH":50}.get(signal,999)

def analyze(ticker, name):
    try:
        df=yf.download(ticker,period="1y",interval="1d",progress=False,auto_adjust=True)
        if df is None or df.empty: return None
        if isinstance(df.columns,pd.MultiIndex): df.columns=df.columns.get_level_values(0)
        df=df.loc[:,~df.columns.duplicated()].dropna(subset=['Close','High','Low'])
        if len(df)<40: return None
        close=[float(x) for x in df['Close'].tolist()]
        high=[float(x) for x in df['High'].tolist()]
        low=[float(x) for x in df['Low'].tolist()]
        open_=[float(x) for x in df['Open'].tolist()] if 'Open' in df.columns else close[:]
        price=close[-1]; kama_s=calc_kama(close); kama_v=kama_s[-1]
        k_pct=round((price/kama_v-1)*100 if kama_v else 0,2)
        er=calc_er(close); ao,baff=calc_ao_baffetti(high,low)
        rsi_v=calc_rsi(close); atr_v=calc_atr(high,low,close)
        trail=round(price-TRAIL_MULT*atr_v,3); trend=trendycator(close)
        sar_v,sar_bull=calc_sar(high,low)
        vi_plus,vi_minus,vortex_bull=calc_vortex(high,low,close)
        rvi_val,rvi_sig,rvi_bull=calc_rvi(close,open_,high,low)
        p7=round((price/close[-8]-1)*100 if len(close)>=8 else 0,2)
        p30=round((price/close[-31]-1)*100 if len(close)>=31 else 0,2)
        cross=0
        for i in range(len(kama_s)-1,0,-1):
            if (close[i]>kama_s[i])==(close[-1]>kama_v): cross+=1
            else: break
        score=calc_score(er,baff,k_pct,p7,p30,ao>0,cross,trend,vortex_bull,rvi_bull)
        signal=calc_signal(price,kama_v,er,baff,trend,ao,sar_bull,vortex_bull,rvi_bull)
        q=qualifies(signal,score,price,kama_v)
        return {"ticker":ticker,"name":name,"price":round(price,3),"kama":round(kama_v,3),
                "k_pct":k_pct,"er":er,"ao":ao,"baffetti":baff,"rsi":rsi_v,"atr":atr_v,
                "trailing_stop":trail,"trend":trend,"sar":sar_v,"sar_bullish":sar_bull,
                "vi_plus":vi_plus,"vi_minus":vi_minus,"vortex_bullish":vortex_bull,
                "rvi":rvi_val,"rvi_signal":rvi_sig,"rvi_bullish":rvi_bull,
                "perf7":p7,"perf30":p30,"score":score,"signal":signal,"qualifies":q}
    except Exception as e:
        print(f"  x {ticker}: {e}"); return None

STATE_FILE="portfolio_state.json"

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f: return json.load(f)
    return {k:{"positions":[],"history":[]} for k in GROUPS}

def save_state(state):
    with open(STATE_FILE,"w") as f: json.dump(state,f,indent=2,default=str)

def update_portfolio(gk, existing, candidates, today_str):
    today=date.fromisoformat(today_str); kept=[]; exited=[]
    for pos in existing:
        cur=next((c for c in candidates if c["ticker"]==pos["ticker"]),None)
        if cur is None: pos["warning"]="Dati non disponibili"; kept.append(pos); continue
        entry_date=date.fromisoformat(pos["entry_date"]); days_held=(today-entry_date).days
        cur_gain=round((cur["price"]/pos["entry_price"]-1)*100,2)
        new_trail=round(cur["price"]-TRAIL_MULT*cur["atr"],3)
        trail_stop=max(pos.get("trailing_stop",new_trail),new_trail)
        exit_reason=None
        if cur["k_pct"]<0: exit_reason="K% negativo"
        elif cur["price"]<=trail_stop: exit_reason="Trailing Stop"
        elif cur["score"]<18: exit_reason="Score < 18"
        elif days_held>=TIME_STOP: exit_reason=f"Time Stop {days_held}gg"
        if exit_reason:
            exited.append({**pos,"exit_reason":exit_reason,"exit_price":cur["price"],"final_gain_pct":cur_gain})
            continue
        pos.update({"current_price":cur["price"],"current_gain_pct":cur_gain,"days_held":days_held,
                    "trailing_stop":trail_stop,"pre_alert":days_held>=PRE_ALERT and cur_gain<TARGET_PCT,
                    "target_hit":cur_gain>=TARGET_PCT,"signal":cur["signal"],"score":cur["score"],
                    "er":cur["er"],"trend":cur["trend"],"sar_bullish":cur["sar_bullish"],
                    "vortex_bullish":cur.get("vortex_bullish",False),"rvi_bullish":cur.get("rvi_bullish",False),
                    "perf7":cur["perf7"],"perf30":cur["perf30"],"warning":None})
        kept.append(pos)
    slots=MAX_POS-len(kept); existing_t={p["ticker"] for p in kept}
    new_q=sorted([c for c in candidates if c["qualifies"] and c["ticker"] not in existing_t],
                 key=lambda x:x["er"],reverse=True)
    for c in new_q[:slots]:
        ep=c["price"]; stop=round(max(ep-TRAIL_MULT*c["atr"],c["trailing_stop"]),3)
        kept.append({"ticker":c["ticker"],"name":c["name"],"entry_date":today_str,"entry_price":ep,
                     "current_price":ep,"target_price":round(ep*(1+TARGET_PCT/100),3),
                     "stop_loss":stop,"trailing_stop":stop,"current_gain_pct":0.0,"days_held":0,
                     "pre_alert":False,"target_hit":False,"score":c["score"],"er":c["er"],
                     "signal":c["signal"],"trend":c["trend"],"sar_bullish":c["sar_bullish"],
                     "vortex_bullish":c.get("vortex_bullish",False),"rvi_bullish":c.get("rvi_bullish",False),
                     "perf7":c["perf7"],"perf30":c["perf30"],"warning":None})
    total_score=sum(p["score"] for p in kept)
    for p in kept: p["weight_pct"]=round(p["score"]/total_score*100,1) if total_score else 0
    return kept,exited

def main():
    now=datetime.now(ROME_TZ); today_str=now.date().isoformat()
    print(f"🦅 RAPTOR TEMATICI — {now.strftime('%d/%m/%Y %H:%M')} CET")
    state=load_state(); output_groups={}
    for gk,gcfg in GROUPS.items():
        print(f"\n📊 {gcfg['name']}...")
        data=[]
        for ticker,name in gcfg["tickers"].items():
            print(f"  {ticker}...",end=" ",flush=True)
            r=analyze(ticker,name)
            if r: data.append(r); print(f"✓ s={r['score']} {r['signal']}")
            else: print("✗")
        if gk not in state: state[gk]={"positions":[],"history":[]}
        positions,exited=update_portfolio(gk,state[gk].get("positions",[]),data,today_str)
        state[gk]["positions"]=positions
        state[gk].setdefault("history",[]).extend(exited)
        output_groups[gk]={"name":gcfg["name"],"color":gcfg["color"],"positions":positions,
                           "use_xeon":len(positions)==0,
                           "qualified":[d for d in data if d["qualifies"]],
                           "watchlist":sorted([d for d in data if not d["qualifies"]],
                                              key=lambda x:x["score"],reverse=True)[:6],
                           "all":sorted(data,key=lambda x:x["score"],reverse=True)}
        print(f"  -> {len(positions)} pos {'| XEON' if not positions else ''}")
    print("\n💰 XEON...")
    xeon=analyze(XEON_TICKER,"Xtrackers EUR Overnight Rate Swap UCITS ETF")
    save_state(state)
    output={"updated_at":now.isoformat(),"updated_display":now.strftime("%d/%m/%Y %H:%M CET"),
            "xeon_price":xeon["price"] if xeon else None,"groups":output_groups}
    with open("tematici.json","w") as f: json.dump(output,f,indent=2,default=str)
    print(f"\n✅ tematici.json — {sum(len(g['positions']) for g in output_groups.values())} posizioni totali")

if __name__=="__main__":
    main()
