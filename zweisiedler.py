
# coding: utf-8

#########################################################
# GENERAL FUNCTIONS
# Created by Damian von Schoenborn on November 14, 2015
# #######################################################


#########################################################
# setup
#########################################################

def importModules(modules=[]):
    #Not yet working, may not need
    import logging
    for module in modules:
        try: import module
        except: logging.critical('need module %s' % module)

def setSparkContext():
    '''Usage: sc = setSparkContext()'''
    from pyspark import SparkConf, SparkContext
    # configuration options can be found here: http://spark.apache.org/docs/latest/configuration.html
    conf = (SparkConf()
             .setMaster("local[7]")
             .setAppName("uyulala")
             .set("spark.executor.memory", "14g"))
    return SparkContext(conf = conf)

def setSparkSQL():
    '''Usage: sqlContext = setSparkSQL()'''
    from pyspark.sql import SQLContext
    return SQLContext(setSparkContext())


#########################################################
# Asset Data
#########################################################

def assetList(assets='Test'):
    #globalIndicies = ['^GSPC','^DJI','^IXIC','^NYA','^XAX','^BUK100P','^RUT','^VIX','^FTSE','^GDAXI','^FCHI','^STOXX50E','^N100','^BFX','IMOEX.ME','^N225','^HSI','000001.SS','^STI','^AXJO','^AORD','^BSESN','^JKSE','^KLSE','^NZ50','^KS11','^TWII','^GSPTSE','^BVSP','^MXX','^IPSA','^MERV','^TA125.TA','^CASE30','^JN0U.JO',^DJGSP]
    #Forex = ['BTCUSD=X','ETHUSD=X','EURUSD=X','JPY=X','GBPUSD=X','AUDUSD=X','NZDUSD=X','EURJPY=X','GBPJPY=X','EURGBP=X','EURCAD=X','EURSEK=X','EURCHF=X','EURHUF=X','EURJPY=X','CNY=X','HKD=X','SGD=X','INR=X','MXN=X','PHP=X','IDR=X','THB=X','MYR=X','ZAR=X','RUB=X']
    #Treasuries=['^IRX','^FVX','^TNX','^TYX']
    if assets == 'SchwabOneSource':
        return ['SCHB','SCHX','SCHV','SCHG','SCHA','SCHM','SCHD','SMD','SPYD','MDYV','SLYV','QUS','MDYG','SLYG','DEUS','ONEV','ONEY','SHE','RSP','XLG','ONEO','SPYX','FNDX','FNDB','SPHB','FNDA','SPLV','DGRW','RFV','RPV','RZG','RPG','RZV','RFG','DGRS','SDOG','EWSC','KRMA','JHML','QQQE','RWL','RWJ','RWK','JPSE','WMCR','DWAS','SYG','SYE','SYV','DEF','JHMM','RDIV','PDP','PKW','KNOW','JPUS','JPME','ESGL','SCHF','SCHC','SCHE','FNDF','ACIM','FEU','QCAN','LOWC','QEFA','QGBR','QEMM','QJPN','QDEU','CWI','IDLV','DBEF','HFXI','DEEF','FNDE','FNDC','WDIV','JPN','DDWM','DBAW','EELV','HDAW','DBEZ','DWX','HFXJ','HFXE','JHDG','DXGE','EDIV','GMF','PAF','IDOG','DEMG','PID','DXJS','IHDG','DNL','EUSC','GXC','EDOG','DGRE','EWX','DBEM','JHMD','CQQQ','JPIN','EWEM','EEB','PIN','PIZ','PIE','JPGE','JPEU','HGI','FRN','JPEH','JPIH','JPEM','ESGF','SCHZ','SCHP','SCHR','SCHO','TLO','ZROZ','FLRN','SHM','AGGY','CORP','AGZD','BSCQ','BSCJ','BSCH','BSCK','BSCL','BSCO','BSCN','BSCP','BSCM','BSCI','HYLB','RVNU','TFI','BWZ','PGHY','AGGE','CJNK','CWB','HYLV','BSJO','BSJJ','BSJM','BSJL','BSJK','BSJN','BSJI','BSJH','HYZD','AGGP','DSUM','BWX','PCY','PHB','HYMB','IBND','HYS','DWFI','BKLN','SRLN','SCHH','NANR','RTM','RYT','RHS','GNR','GII','RGI','EWRE','RYU','RYE','RYH','RCD','RYF','GHII','MLPX','MLPA','RWO','SGDM','RWX','PBS','CGW','ENFR','BOTZ','PSAU','CROP','GRES','JHMF','JHMT','JHMH','JHMC','JHMI','JHMA','JHME','JHMS','JHMU','ZMLP','GAL','FXY','FXS','FXF','FXE','FXC','FXB','FXA','PUTW','PSK','USDU','PGX','VRP','DYLS','INKM','RLY','WDTI','MNA','CVY','QMN','QAI','LALT','SIVR','SGOL','GLDW','PPLT','PALL','GLTR','USL','GCC','USCI','BNO','UGA','UNL','CPER']
    elif assets == 'AllStocks':
        import pandas
        '''try:
            nasdaq = pandas.read_csv('https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download').Symbol.tolist()
            amex = pandas.read_csv('https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download').Symbol.tolist()
            nyse = pandas.read_csv('https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download').Symbol.tolist()
            return list(set([item.split('.')[0].split('^')[0].strip() for item in nasdaq+amex+nyse]))'''
        try:
            fullList = ['TXG','YI','PIH','PIHPP','TURN','FLWS','GOED','BCOW','ONEM','FCCY','SRCE','VNET','XXII','TWOU','QFIN','KRKR','DDD','MMM','WBAI','JOBS','WUBA','ETNB','JFK','JFKKR','JFKKU','JFKKW','EGHT','EGHT','NMTR','JFU','AHC','AOS','ATEN','AAON','AIR','AAN','ABB','ABT','ABBV','ABEO','ANF','FAX','IAF','AEF','AGD','FCO','AWP','ACP','JEQ','ASGI','AOD','ABMD','ABM','AXAS','ACIU','ACIA','ACTG','ACHC','ACAD','AKR','ACAM','ACAMU','ACAMW','ACST','ACEL','AXDX','ACCP','XLRN','ACN','ACCO','ACCD','ARAY','ACEVU','ACLL','ACRX','ACER','ACHV','ACIW','ACRS','ACMR','ACU','ACNB','ACOR','ATV','ATNM','ATVI','AYI','GOLF','AFIB','ADMS','ADMP','ADX','PEO','AE','AHCO','ADAP','ADPT','ADCT','ADXN','ADUS','AEY','AGRO','ADIL','ADILW','ACET','ADNT','ADTX','ADMA','ADBE','ADT','ATGE','ADTN','ADRO','AAP','ADSW','WMS','ADES','AEIS','AMD','ASIX','ADXS','AVK','ADVM','DWEQ','DWAW','DWUS','DWMC','DWSH','ACT','ACM','AEGN','AGLE','AEFC','AEB','AEG','AEHR','AMTX','AER','AERI','ACY','AJRD','AVAV','ARPO','AIH','AEZS','AEMD','AMG','MGR','AFMD','AFL','AFYA','MITT','MITT^A','MITT^B','MITT^C','AGBA','AGBAR','AGBAU','AGBAW','AGCO','UAVS','AGEN','AGE','AGRX','A','AGYS','AGIO','AGMH','AGNC','AGNCM','AGNCN','AGNCO','AGNCP','AEM','API','ADC','AGFS','AIKI','ALRN','AIMT','AIRI','AL','AL^A','APD','AIRT','AIRTP','AIRTW','ATSG','AIRG','ANTE','AKAM','AKTX','AKCA','AKBA','KERN','KERNW','AKRO','AKER','AKUS','AKTS','AKU','ALP^Q','ALG','AGI','ALRM','ALK','ALSK','AIN','ALB','ALAC','ALACR','ALACU','ALACW','ACI','ALBO','AA','ALC','ALDX','ALEC','ALRS','ALEX','ALX','ARE','AXU','ALXN','AQN','AQNA','AQNB','BABA','ALCO','ALIT','ALGN','ALIM','ALYA','ALJJ','ALKS','ALLK','Y','ATI','ABTX','ALGT','ALLE','ALNA','ALE','ADS','AFB','ARLP','AWF','AB','LNT','AIO','CBH','NCV','NCV^A','NCZ','NCZ^A','ACV','NFJ','NIE','AESE','AHPI','AMOT','ALSN','ALLO','ALLT','ALVR','MDRX','ALL','ALL^B','ALL^G','ALL^H','ALL^I','ALLY','ALLY^A','AAU','ALNY','AOSL','AHAC','AHACU','APT','GOOG','GOOGL','SMCP','ATEC','ALPN','PINE','ALTG','ALTG.WS','ALTA','ALTR','ALIN^A','ALIN^B','ALIN^E','ATHE','AYX','ATUS','ALT','AAMC','ASPS','AIMC','MO','ALTM','ACH','ALUS','ALUS.U','ALUS.WS','ALXO','AMAG','AMAL','AMRN','AMRK','AMZN','AMBC','AMBC','AMBC.WS','AMBA','ABEV','AMBO','AMC','AMCX','AMCI','AMCIU','AMCIW','DIT','AMCR','DOX','AMED','AMTB','AMTBB','UHAL','AEE','AMRC','AMRH','AMRHW','ATAX','AMOV','AMOV','AMX','AAL','AAT','AXL','ACC','AEO','AEP','AEP^B','AEP^C','AEL','AEL^A','AEL^B','AXP','AFIN','AFINP','AFG','AFGB','AFGC','AFGD','AFGH','AMH','AMH^D','AMH^E','AMH^F','AMH^G','AMH^H','AIG','AIG.WS','AIG^A','AMNB','ANAT','AOUT','APEI','ARL','ARA','AREC','AMRB','AMS','AMSWA','AWR','AMSC','AMT','AVD','AVCT','AVCTW','AWK','AMWD','CRMT','USAS','COLD','AMP','ABCB','AMSF','ASRV','ASRVP','ABC','ATLO','AMGN','FOLD','AMKR','AMN','AMRX','AP','AMPH','APH','AMPE','IBUY','AMHC','AMHCU','AMHCW','AXR','AMYT','HKIB','ASYS','AME','AMRS','ADI','PLAN','ANAB','AVXL','ANCN','ANDA','ANDAR','ANDAU','ANDAW','DYFN','FINS','ANGI','ANGO','AU','BUD','ANIP','ANIK','ANIX','NLY','NLY^D','NLY^F','NLY^G','NLY^I','ANNX','ANVS','ANPC','ANSS','ATRS','ATEX','AM','AR','ANTM','ANH','ANH^A','ANH^B','ANH^C','AON','APA','AIV','APLS','APEX','APXT','APXTU','APXTW','APHA','APG','APOG','ARI','APEN','APO','APO^A','APO^B','AINV','AMEH','AFT','AIF','APPF','APPN','APLE','AAPL','APDN','AGTC','AIT','AMAT','AMTI','AAOI','APLT','AUVI','APRE','ATR','APVO','APTX','APTV','APTV^A','APM','APTO','APYX','AQMS','AQB','AQST','ARMK','ARAV','ABR','ABR^A','ABR^B','ABR^C','ABUS','ARC','ABIO','RKDA','ARCB','MT','MTCN','ACGL','ACGLO','ACGLP','ARCH','ADM','AROC','FUV','ARCE','ARNC','ARCO','ACA','ARCT','RCUS','ARQT','ARD','ARDX','ASC','ARNA','AFC','ARCC','ACRE','ARDC','ARES','ARES^A','AGX','ARGX','ARGD','ARGO','ARGO^A','ARDS','ANET','ARKR','AI','AI^B','AI^C','AIC','AIW','ARLO','AHH','AHH^A','ARMP','ARR','ARR^C','AFI','AWI','DWCR','DWAT','ARW','AROW','ARWR','ARTL','ARTLW','ARTNA','AJG','APAM','AACQ','AACQU','AACQW','ARTW','ARVN','ARYB','ARYBU','ARYBW','ARYA','ASA','ABG','ACND','ACND.U','ACND.WS','ASND','ASX','ASGN','AHT','AHT^D','AHT^F','AHT^G','AHT^H','AHT^I','AINC','ASH','APWC','ASLN','ASML','ASPN','ASPU','AHL^C','AHL^D','AHL^E','AZPN','AWH','ASMB','ASRT','AMK','ASB','ASB^C','ASB^D','ASB^E','ASB^F','AC','AIZ','AIZP','AGO','AGO^B','AGO^E','AGO^F','ASFI','ASTE','AZN','ATRO','ALOT','ASTC','ASUR','HOME','T','T^A','T^C','TBB','TBC','AACG','ATRA','ATTO','ATH','ATH^A','ATH^B','ATH^C','ATNX','ATHX','ATIF','ATKR','AAME','ACBI','AT','AUB','AUBAP','AY','ATLC','AAWW','ATCO','ATCO^D','ATCO^E','ATCO^G','ATCO^H','ATCO^I','ATCX','ATCXW','TEAM','ATO','ATNI','ATOM','ATOS','BCEL','ATRC','ATRI','LIFE','AUBN','AUDC','AEYE','AUPH','EARS','ACB','JG','AUG','ADSK','ATHM','ALV','AUTL','ADP','AN','AUTO','AZO','AVDL','AHI','AVLR','AVCO','AWX','AVB','AGR','AVNS','AVTR','AVTR^A','AVYA','ATXI','AVEO','AVY','AVNW','CDMO','CDMOP','AVID','RNA','AVNT','AVGR','ASM','CAR','AVA','RCEL','AVT','AVRO','AWRE','AXTA','ACLS','AXLA','AXS','AXS^E','AXGN','AAXN','AXNX','AX','AXO','AXGT','AXSM','AXTI','AYLA','AYRO','AYTU','AZUL','AZRE','AZRX','AZZ','BGS','RILY','RILYG','RILYH','RILYI','RILYL','RILYM','RILYN','RILYO','RILYP','RILYZ','BMRG','BMRG.U','BMRG.WS','BOSC','BTG','BW','BGH','BMI','BIDU','BCSF','BKR','BCPC','BBN','BLL','BTN','BLDP','BANC','BANC^D','BANC^E','BANF','BANFP','BBAR','BBVA','BBD','BBDO','BCH','BLX','BSBR','BSAC','BSMX','SAN','SAN^B','CIB','BXS','BXS^A','BCV','BCV^A','BAND','BFC','BAC','BAC^A','BAC^B','BAC^C','BAC^E','BAC^K','BAC^L','BAC^M','BAC^N','BML^G','BML^H','BML^J','BML^L','BOCH','BOH','BMRC','BMLP','BMO','NTB','BNS','BKSC','BOTJ','OZK','BSVN','BFIN','BKU','BWFG','BANR','BZUN','BHB','BCS','DFVL','DFVS','TAPR','BBDC','MCI','MPV','BNED','B','BRN','BBSI','GOLD','GOLD','BSET','ZTEST','BATL','BXRX','BHC','BAX','BCML','BTE','BBQ','BBX','BCBP','BCE','BCTG','BECN','BEEM','BEEMW','BEAM','BBGI','BZH','BDX','BDXB','BBBY','BGNE','BELFA','BELFB','BDC','BLPH','BLCM','BRBR','BLU','BHE','BNFT','BNTC','BLI','BRK.A','BRK.B','BHLB','BRY','BERY','BBY','BEST','BWMX','XAIR','BYND','BYSI','BGSF','BGCP','BHP','BBL','BCYC','BGFV','BIG','BRPA','BRPAR','BRPAU','BRPAW','BIGC','BH','BH.A','BILI','BILL','BASI','BCDA','BCDAW','BIOC','BIOX','BCRX','BDSI','BFRA','BIIB','BHVN','BHTG','BKYI','BIOL','BLFS','BLRX','BMRN','BMRA','PHGE','PHGE.U','PHGE.WS','BNGO','BNGOW','BVXV','BNTX','BPTH','BIO','BIO.B','BSGM','BSTC','TECH','BEAT','BIVI','BTAI','BGI','BITA','BJRI','BJ','BKTI','BDTX','BKH','BKI','BSM','BLKB','BB','BL','BGIO','BFZ','CII','BKCC','BHK','HYT','BTZ','DSU','BGR','BDJ','EGF','FRA','BFO','BGT','BOE','BME','BMEZ','BAF','BKT','BGY','BKN','BTA','BZM','MHE','BIT','MUI','MUA','BKK','BBK','BBF','BYM','BFK','BLE','BTT','MEN','MUC','MUH','MHD','MFL','MUJ','MHN','MUE','MUS','MVT','MYC','MCA','MYD','MYF','MFT','MIY','MYJ','MYN','MPA','MQT','MYI','MQY','BNY','BQH','BSE','BFY','BCX','BST','BSTZ','BSD','TCPC','BUI','BHV','BLK','BGB','BGX','BSL','BLNK','BLNKW','BDR','BE','BLMN','BCOR','APRN','BLBD','BHAT','BRBS','BLUE','BLCT','BXG','BKEP','BKEPP','BXC','BPMC','BRG','BRG^A','BRG^C','BRG^D','ITEQ','BMCH','DCF','DHF','DMB','DMF','DSM','LEO','BA','BSBK','WIFI','BCC','BOKF','BOKFL','BCEI','BNSO','BKNG','BOOT','BAH','BIMI','BWA','BRQS','BORR','SAM','BOMN','BPFH','BXP','BXP^B','BSX','BSX^A','EPAY','BWL.A','BOWXU','BOX','BOXL','BYD','BPMP','BP','BPT','BRC','BBRX','BHR','BHR^B','BHR^D','BCLI','BWAY','BDN','BWG','LND','BAK','BRFS','BCTX','BBI','BDGE','BBIO','BLIN          ','BWB','BRID','MNRL','BFAM','BEDU','BCOV','BHF','BHFAL','BHFAO','BHFAP','BSA','BSIG','BV','BRLI','BRLIR','BRLIU','BRLIW','EAT','BCO','BMY','BMY~','VTOL','BTI','BRX','AVGO','AVGOP','BRMK','BRMK.WS','BR','BSN.U','BYFC','BWEN','BROG','BROGW','BKD','BAM','BBU','DTLA^','BIPC','BIP','BPY','BPYPN','BPYPO','BPYPP','BPYU','BPYUP','RA','BEPC','BEP','BEP^A','BRKL','BCAC','BCACU','BRKS','BRO','BF.A','BF.B','BRP','DOOO','BRT','BRKR','BC','BC^A','BC^B','BC^C','BMTC','BSQR','BKE','BVN','BBW','BLDR','BG','BTAQU','BURL','BNR','BFST','BWXT','BY','CFFI','CHRW','CABA','CABO','CBT','CCMP','COG','CACI','WHD','CADE','CDNS','CDZI','CAE','CZR','CSTE','CAI','CAI^A','CAI^B','CLBS','CHY','CHI','CCD','CHW','CGO','CPZ','CSQ','CAMP','CVGW','CMCL','CAL','CALB','CWT','CALA','CALX','ELY','CALT','CPE','CALM','CLMT','CLXT','CEI','CMBM','CATC','CAC','CPT','CCJ','CPB','CWH','CAMT','CAN','GOOS','CM','CNI','CNQ','CP','CSIQ','CGIX','CANF','CANG','CNNE','CAJ','CGC','CMD','CPHC','CBNK','CCBG','COF','COF^F','COF^G','COF^H','COF^I','COF^J','CPLP','CSU','CSWC','CSWCL','BXMT','CPTA','CPTAG','CPTAL','CFFN','CPRI','CAPR','CSTR','CPSR','CPSR.U','CPSR.WS','CMO','CMO^E','CPST','CARA','CRDF','CAH','CSII','CDLX','CATM','CDNA','CTRE','CARG','CSL','KMX','CCL','CUK','PRTS','CRS','CSV','CARR','TAST','CARS','CARE','CRI','CVNA','CARV','CASA','CWST','CASY','CASI','CSPR','CASS','SAVA','CSTL','CSLT','CTRM','CATB','CTLT','CBIO','CPRX','CTT','CAT','CATY','CATO','CVCO','CBFV','CBAT','YCBD','YCBD^A','CBZ','CBL','CBL^D','CBL^E','CBMB','CBO','CBOE','IGR','CBRE','CBTX','CBX','PCPL','PCPL.U','PCPL.WS','PRPB.U','CDK','CDW','CECE','FUN','CDR','CDR^B','CDR^C','CE','CELC','CLS','CELG~','CEL','CLDX','APOP','APOPW','CLRB','CLRBZ','CLLS','CBMG','CVM','CLSN','CELH','CYAD','CPAC','CX','CETX','CETXP','CETXW','CVE','CNC','CDEV','CEN','CNP','CNP^B','CNTG','EBR','EBR.B','CETV','CENT','CENTA','CEPU','CET','CVCY','CNTX','LEU','CENX','CNBKA','CNTY','CCS','CTL','CRNT','CERC','CRNC','CDAY','CERN','CERS','CEVA','CFBK','CFFA','CFFAU','CFFAW','CFIIU','CF','GIB','CSBR','CHX','CHNG','CHNGU','ECOM          ','CHRA','CHAQ','CHAQ.U','CHAQ.WS','CTHR','CRL','GTLS','CHTR','CCF','CLDT','CHKP','CHEK','CHEKZ','CMPI','CKPT','CMCM','CHGG','CEMI','CHE','CCXI','CC','CHMG','CQP','LNG','CHMI','CHMI^A','CHMI^B','CPK','CVX','CHWY','CHFS','CHMA','CVR','CSSE','CSSEN','CSSEP','CHS','PLCE','CIM','CIM^A','CIM^B','CIM^C','CIM^D','CMRX','CAAS','CBPO','CCCL','CCRC','DL','CEA','JRJC','CHN','CGA','HGSH','CIH','CJJD','CLEU','LFC','CHL','CHNR','COE','SNP','CPHI','CREG','ZNH','SXTC','CHA','CHU','CXDC','PLIN','CYD','CNET','IMOS','CMG','CHH','COFS','CHPM','CHPMU','CHPMW','CDXC','CHSCL','CHSCM','CHSCN','CHSCO','CHSCP','CB','CHT','CHD','CCX','CCX.U','CCX.WS','CCXX','CCXX.U','CCXX.WS','CCIV.U','CHDN','CHUY','CDTX','CIEN','CI','CIIC','CIICU','CIICW','CMCT','CMCTP','XEC','CMPR','CNNB','CBB','CBB^B','CINF','CIDM','CNK','CINR','CTAS','CIR','CRUS','CSCO','CIT','CIT^B','CTRN','CCAC','CCAC.U','CCAC.WS','BLW','C','C^J','C^K','C^N','C^S','CTXR','CTXRW','CZNC','CZWI','CFG','CFG^D','CFG^E','CIZN','CIA','CTXS','CHCO','CIO','CIO^A','CVEO','CIVB','CKX','CCC','CLAR','CLNE','CLH','CLSK','CCO','CACG','YLDE','EMO','LRGE','CEM','CTR','CLFD','CLRO','CLPT','CLSD','CLIR','CLW','CWEN','CWEN.A','CBLI','CLF','CLPR','CLX','CLDR','NET','GLV','GLQ','GLO','CLVS','CLPS','CMLFU','CME','CMS','CMS^B','CMSA','CMSC','CMSD','CNA','CCNE','CCNEP','CNF','CNHI','CNO','CEO','CNSP','CNXM','CNX','CCB','KOF','KO','COKE','CCEP','COCP','CODA','CCNC','CDXS','CODX','CVLY','CDE','JVA','CCOI','CGNX','CTSH','CWBR','COHN','FOF','CNS','UTF','LDP','MIE','RQI','RNP','PSF','RFI','COHR','CHRS','CRHC.U','COHU','CFX','CFXA','CL','CGRO','CGROU','CGROW','CLCT','COLL','CIGI','CLGN','CXE','CIF','CXH','CMU','CLA.U','CBAN','CLNY','CLNY^G','CLNY^H','CLNY^I','CLNY^J','CLNC','HHT','COLB','CLBK','CXP','STK','COLM','CMCO','CCZ','CMCSA','CMA','FIX','CBSH','CMC','CVGI','COMM','JCS','CBU','ESXB','CFBI','CYH','CHCT','CTBI','CWBC','CVLT','CIG','CIG.C','CBD','SBS','ELP','CCU','CODI','CODI^A','CODI^B','CODI^C','CMP','CGEN','CPSI','CTG','CIX','SCOR','CHCI','LODE','CRK','CMTL','CAG','CNCE','CXO','CCM','BBCP','CDOR','CDOR','CNDT','CFMS','CNFR','CNFRL','CNMD','CNMD','CNOB','CONN','COP','CCR','CEIX','CNSL','ED','CWCO','STZ','STZ.B','CNST','CSTM','ROAD','CPSS','TCS','MCF','CLR','CFRX','VLRS','CTRA','CPTI','CPAA','CPAAU','CPAAW','CTB','CPS','CTK','CPA','CPRT','CRBP','CORT','CLB','CMT','CXW','CLGX','CORE','CORR','CORR^A','CPLG','COR','CRMD','CNR','CSOD','CRF','CLM','GLW','CAAP','GYC','OFC','CTVA','CRTX','CLDB','CRVL','KOR','CRVS','CZZ','CMRE','CMRE^B','CMRE^C','CMRE^D','CMRE^E','CSGP','COST','COTY','CPAH','ICBK','COUP','CUZ','CVA','CVLG','CVET','COWN','COWNL','COWNZ','CPF','CVU','CPSH','CRAI','CBRL','BREW','CR','CRD.A','CRD.B','CRTD','CRTDW','CREX','CREXW','BAP','CACC','GLDI','SLVO','USOI','CIK','CS','DHY','CREE','CRSA','CRSAU','CRSAW','CCAP','CPG','CEQP','CEQP^','CRESY','CXDO','CRHM','CRH','CRNX','CRSP','CRTO','CROX','CRON','CCRN','CRT','CAPL','CFB','CRWD','CCI','CRWS','CCK','CRY','CYRX','CSGS','CCLP','CSPI','CSWI','CSX','CTIC','CTO','CTS','CUBE','CUB','CUE','CFR','CULP','CPIX','CMI','CMLS','CVAC','CRIS','CURO','CW','SRV','SZC','CWK','CUBB','CUBI','CUBI^C','CUBI^D','CUBI^E','CUBI^F','CUTR','CVBF','CVV','CVI','UAN','CVS','CYAN','CYBR','CYBE','CYCC','CYCCP','CYCN','CBAY','CTEK','CELP','CYRN','CONE','CYTK','CTMX','CTSO','DHI','DEH','DEH.U','DEH.WS','DADA','DJCO','DAKT','DAN','DHR','DHR^A','DHR^B','DAC','DQ','DRI','DARE','DRIO','DRIOW','DAR','DSKE','DSKEW','DAIO','DDOG','DTSS','PLAY','DTEA','DFNL','DINT','DUSA','DWLD','DVA','DWSN','DXR','DBVT','DCP','DCP^B','DCP^C','DCPH','DECK','DE','DFHT','DFHTU','DFHTW','IBBJ','TACO','DEX','VCF','DDF','VFL','VMM','DCTH','DKL','DK','DELL','DLPH','DAL','DLA','DLX','DNLI','DNN','DENN','XRAY','DRMT','DMTK','DBI','DESP','DXLG','DSWL','DB','DVN','DXCM','DFPH','DFPHU','DFPHW','DHX','DHT','DEO','DMAC','DHIL','DSSI','FANG','DPHC','DPHCU','DPHCW','DRH','DRH^A','DSX','DSX^B','DRNA','DKS','DBD','DFFN','DGII','DMRC','DRAD','DRADP','DGLY','DMS','DMS.WS','DLR','DLR^C','DLR^G','DLR^J','DLR^K','DLR^L','APPS','DDS','DDT','DCOM','DCOMP','DIN','DIOD','DRTT','DFS','DISCA','DISCB','DISCK','DISH','DHC','DHCNI','DHCNL','DNI','DLHC','BOOM','DMYT','DMYT.U','DMYT.WS','DMYD.U','DSS','DOCU','DOGZ','DLB','DG','DLTR','DLPN','DLPNW','D','DCUE','DRUA','DPZ','DOMO','UFS','DCI','DGICA','DGICB','DFIN','DMLP','LPG','DORM','DDI','DSL','DBL','DLY','PLOW','DEI','DOYU','DOV','DVD','DOW','DPW','RDY','DKNG','LYL','DGNR.U','DRD','DRQ','DS','DS^B','DS^C','DS^D','DBX','DSPG','DTE','DTJ','DTP','DTQ','DTW','DTY','DCT','DCO','DSE','DNP','DTF','DUC','DPG','DUK','DUK^A','DUKB','DUKH','DRE','DLTH','DNB','DNKN','DXF','DUOT','DD','DRRX','DXC','DXPE','DYAI','DY','DLNG','DLNG^A','DLNG^B','DT','DYNT','DVAX','DYN','DX','DX^B','DX^C','DZSI','ETFC','CTA^A','CTA^B','ELF','ETACU','SSP','EBMT','EGBN','EGLE','GRF','EXP','EGRX','ECC           ','ECCB','ECCX','ECCY','EIC','IGLE','ESTE','ERES','ERESU','ERESW','ESSC','ESSCR','ESSCU','ESSCW','EWBC','DEA','EML','EGP','EMN','KODK','EAST','ETN','EVM','CEV','ETV','ETW','EV','EOI','EOS','EFT','EFL','EFF','EHT','EVV','EIM','ETX           ','EOT','EVN','ENX','EVY','EVGBC','EVSTC','EVLMC','ETJ','EFR','EVF','EVG','EVT','ETO','ETG','ETB','EXD','ETY','EXG','EBON','EBAY','EBAYL','EBIX','ECHO','SATS','MOHO','ECL','EC','EDAP','EDSA','EPC','EIX','EDNT','EDIT','EDUC','EW','EGAN','EH','EHTH','EIDX','EIGR','EKSO','EP^C','LOCO','ELAN','ELAT','ESTC','ESLT','EGO','SOLO','SOLOW','ECOR','ELMD','EA','ELSE','ESI','ELVT','LLY','EFC','EFC^A','EARN','ELLO','ECF','ECF^A','ESBK','ELOX','ELTK','EMAN','AKO.A','AKO.B','ERJ','EMCF','EME','EMKR','EEX','EBS','EMR','MSN','ESRT','EIG','EDN','EMX','ENBL','ENTA','ENB','ENBA','EHC','ECPG','WIRE','DAVA','EXK','ENDP','NDRA','NDRAW','EIGI','ENIA','ENIC','ENR','ENR^A','WATT','EFOI','UUUU','UUUU.WS','ERII','ET','ETP^C','ETP^D','ETP^E','EPAC','ERF','ENS','ENG','E','ENLC','ENLV','EBF','ENOB','ENVA','ENPH','NPO','ENSV','ESGR','ESGRO','ESGRP','ETTX','ENTG','ENTX','ENTXW','ETM','EAB','EAE','EAI','ETR','ELC','ELJ','ELU','EMP','ENJ','ENO','ETI^','EZT','EBTC','EFSC','EPD','EVC','ELA','ENV','NVST','EVA','ENZ','EOG','EPAM','EPZM','PLUS','EPR','EPR^C','EPR^E','EPR^G','EPSN','EQT','EFX','EQ','EQIX','EQNR','EQX','EQH','EQH^A','ETRN','EQBK','EQC','EQC^D','EQD.U','ELS','EQR','EQS','ERIC','ERIE','EROS','ERYP','ESCA','ESE','ESPR','ESP','GMBL','GMBLW','ESQ','ESSA','EPIX','ESNT','EPRT','WTRG','WTRU','ESS','ESTA','EL','VBND','VUSE','VIDI','ETH','ETON','ETSY','CLWT','EDRY','EURN','EEFT','EEA','ESEA','EVBN','EVLO','EB','EVBG','EVR','RE','EVK','EVRG','EVRI','EVER','ES','MRAM','EVTC','EVI','EVOP','EVFM','EVGN','EVOK','EVH','EOLS','EPM','EVOL','AQUA','EXAS','XGN','XAN','XAN^C','ROBO','ENPC.U','XELA','EXEL','EXC','EXFO','XCUR','EXLS','EXPI','EXPE','EXPD','EXPC','EXPCU','EXPCW','EXPO','EXPR','STAY','EXTN','EXR','EXTR','XOM','EYEG','EYEN','EYPT','EZPW','FNB','FNB^E','FLRZ','FFIV','FN','FB','FDS','FICO','FLMN','FLMNW','SFUN','DUO','FANH','FTCH','FARM','FMAO','FMNB','FPI','FPI^B','FAMI','FARO','FST.U','FAST','FSLY','FAT','FATBP','FATBW','FATE','FTHM','FBSS','FBK','FFG','AGM','AGM.A','AGM^A','AGM^C','AGM^D','AGM^E','AGM^F','FRT','FRT^C','FSS','FMN','FHI','FDX','FNHC','FENC','RACE','FOE','GSM','FFBW','FCAU','FGEN','FDBC','ONEQ','FNF','FIS','FMO','FDUS','FDUSG','FDUSL','FDUSZ','FRGI','FITB','FITBI','FITBO','FITBP','FISI','FSRV','FSRVU','FSRVW','FTAC','FTACU','FTACW','FINV','FEYE','FAF','FBNC','FNLC','FBP','FRBA','BUSE','FBIZ','FCAP','FCBP','FCNCA','FCNCP','FCF','FCBC','FCCO','FCRD','FCRW','FCRZ','FSLF','FFBC','FFIN','THFF','FFNW','FFWM','FGBI','FHB','FHN','FHN^A','FHN^B','FHN^C','FHN^D','FHN^E','FR','INBK','INBKL','INBKZ','FIBK','AG','FRME','FMBH','FMBI','FMBIO','FMBIP','FXNC','FNWB','FRC','FRC^F','FRC^G','FRC^H','FRC^I','FRC^J','FSFG','FSEA','FSLR','FFA','FMY','FAAR','FPA','BICK','FBZ','FTHI','FCAL','FCAN','FTCS','FCEF','FCA','SKYY','RNDM','FDT','FDTS','FVC','FV','IFV','DDIV','DVOL','DVLU','DWPP','DALI','FDNI','FDEU','FEM','RNEM','FEMB','FEMS','FEN','FIF','FTSM','FEP','FEUZ','FGM','FTGC','FTLB','FSD','HYLS','FTHY','FHK','NFTY','FTAG','FTRI','LEGR','NXTG','FPF','FPXI','FPXE','FJP','FEX','FTC','RNLC','FTA','FLN','LMBS','LDSF','FMB','FMK','FNX','FNY','RNMC','FNK','FEI           ','FAD','FAB','MDIV','MCEF','FMHI','QABA','ROBT','FTXO','QCLN','GRID','CIBR','FTXG','CARZ','FTXN','FTXH','FTXD','FTXL','TDIV','FTXR','QQEW','QQXT','QTEC','FPL','AIRR','RDVY','RFAP','RFDI','RFEM','RFEU','FID','FIV','FCT','FTSL','FYX','FYC','RNSC','FYT','SDVY','FKO','FGB','FCVT','FDIV','FSZ','FIXD','TUSA','FKU','RNDV','FEO','FAM','FUNC','FUSB','MYFW','FCFS','FE','SVVC','FSV','FISV','FIT','FIVE','FPH','FPRX','FVE','FIVN','FVRR','FBC','DFP','PFD','PFO','FFC','FLC','BDL','FLT','FLNG','FLEX','FSI','FLXN','SKOR','ASET','ESG','ESGG','LKOR','QLC','MBSD','FPAY','FLXS','FLIR','FND','FTK','FLO','FLWR','FLS','FLNT','FLDM','FLR','FFIC','FLUX','FLY','FEAC','FEAC.U','FEAC.WS','FMC','FNCB','FOCS','WPF','WPF.U','WPF.WS','BFT.U','FMX','FONR','FL','F','F^B','F^C','FRSX','FOR','FMTX','FORM','FORTY','FORR','FBRX','FRTA','FTNT','FTS','FTV','FTV^A','FBIO','FBIOP','FTAI','FTAI^A','FTAI^B','FVAC','FVAC.U','FVAC.WS','FAII.U','FSM','FBHS','FET','FMCI','FMCIU','FMCIW','FIIIU','FWRD','FORD','FWP','FOSL','FBM','FCPT','FEDU','FOX','FOXA','FOXF','FRAN','FRG','FNV','FC','FELE','FRAF','FTF','BEN','FSP','FT','FI','FRHC','FRLN','FCX','RAIL','FEIM','FREQ','FMS','FDP','FRPT','FRD','RESI','FTDR','FRO','FRPH','FSBW','FSDC','FSK','FSKR','HUGE','FTOCU','FCN','FTSI','FTEK','FCEL','FULC','FLGT','FORK','FLL','FMAX','FULT','FNKO','FUSE','FUSE.U','FUSE.WS','FUSN','FUTU','FTFT','FF','FFHL','FVCB','WILC','GTHX','GCV','GAB','GAB^G','GAB^H','GAB^J','GAB^K','GGZ','GGZ^A','GGT','GGT^E','GGT^G','GUT','GUT^A','GUT^C','GAIA','GLPG','GALT','GRTX','GAU','GLEO','GLEO.U','GLEO.WS','GLMD','GGN','GGN^B','GBL','GNT','GNT^A','GME','GMDA','GLPI','GAN','GCI','GPS','GRMN','GTX','GARS','IT','GLOG','GLOG^A','GLOP','GLOP^A','GLOP^B','GLOP^C','GTES','GATX','GMTA','GLIBA','GLIBP','GCP','GDS','JOB','GNSS','GNK','GENC','GNRC','GAM','GAM^B','GD','GE','GFN','GFNCP','GFNSL','GIS','GMO','GM','GBIO','GCO','GEL','GEN           ','GENE','GTH','GNFT','GNE','GNE^A','GNUS','GMAB','GNMK','GNCA','G','GNPX','GNTX','THRM','GPC','GNW','GEO','GPRK','GPJA','GEOS','GGB','GABC','GERN','GTY','GEVO','GFL','GFLU','ROCK','GIGM','GIX','GIX.U','GIX.WS','GIX~','GIK','GIK.U','GIK.WS','GIII','GILT','GIL','GILD','GBCI','GLAD','GLADD','GLADL','GOOD','GOODM','GOODN','GAIN','GAINL','GAINM','LAND','LANDP','GLT','GKOS','GSK','GLBZ','GBT','GB','GB.WS','CO','GBLI','GBLIL','GMRE','GMRE^A','GNL','GNL^A','GNL^B','GLP','GLP^A','GPN','SELF','GSL','GSL^B','GSLD','GWRS','AIQ','DRIV','POTX','CLOU','KRMA','BUG','DAX','EBIZ','EDUT','FINX','CHIC','GNOM','BFIT','SNSR','LNGR','MILN','EFAS','QYLD','BOTZ','CATH','CEFA','SOCL','ALTY','SRET','EDOC','GXTG','HERO','YLCO','GSAT','GLOB','GL','GL^C','GLBS','GMED','GSMG','GSMGW','GLUU','GLYC','GMS','GOAC.U','GDDY','GOGO','GOCO','GOL','GLNG','GMLP','GMLPP','GFI','GORO','GSV','BTBT','GDEN','AUMN','GOGL','GSS','GV','GSBD','GS','GS^A','GS^C','GS^D','GS^J','GS^K','GS^N','GER','GMZ','GBDC','GTIM','GBLK','GDP','GSHD','GPRO','GHIV','GHIVU','GHIVW','GRSVU','GMHI','GMHIU','GMHIW','GRC','GOSS','GPX','GGG','GRAF','GRAF.U','GRAF.WS','EAF','GHM','GHC','GTE','GRAM','LOPE','GVA','GPMT','GRP.U','GPK','GRVY','GTN','GTN.A','AJX','AJXA','GECC','GECCL','GECCM','GECCN','GEC','GLDD','GPL','GSBC','GWB','GRBK','GDOT','GPP','GPRE','GBX','GRCY','GRCYU','GRCYW','GCBC','GHL','GTEC','GNLN','GLRE','GP','GRNQ','GNRS','GNRSU','GNRSW','GSKY','GHG','GRNV','GRNVR','GRNVU','GRNVW','GEF','GEF.B','GDYN','GDYNW','GSUM','GRIF','GFF','GRFS','GRIN','GRTS','GO','GPI','GRPN','GRWG','GRUB','OMAB','PAC','ASR','AVAL','GGAL','SIM','SUPV','TV','GSAH','GSAH.U','GSAH.WS','GVP','GSIT','GSX','GTT','GTYH','GSH','GNTY','GFED','GH','GHSI','GES','GGM','GPM','GOF','GBAB','GWRE','GIFI','GURE','GPOR','GWPH','GWGH','GXGX','GXGXU','GXGXW','GYRO','HEES','HRB','FUL','HAE','HLG','HOFV','HOFVW','HNRG','HAL','HALL','HALO','HBB','HLNE','HJLI','HJLIW','HWC','HWCPL','HWCPZ','HBI','HNGR','HAFC','HASI','HAPP','HCDI','HONE','HOG','HLIT','HRMY','HMY','HARP','HROW','HSC','HGH','HIG','HIG^G','HBIO','HCAP','HCAPZ','HAS','HVT','HVT.A','HE','HA','HWKN','HWBK','HYAC','HYACU','HYACW','HAYN','HBT','HCHC','HCA','HCI','HDS','HDB','HHR','HCAT','HSAQ','HCCO','HCCOU','HCCOW','HR','HCSG','HTA','HTIA','HQY','PEAK','HSTM','HTLD','HTLF','HTLFP','HTBX','HEBT','HL','HL^B','HEI','HEI.A','HSII','HELE','HLIO','HSDT','HLX','HP','HMTV','AIM','HNNA','HCAC','HCACU','HCACW','HSIC','HEPA','HLF','HRI','HCXY','HCXZ','HTGC','HTBK','HFWA','HGBL','HRTG','HCCI','MLHR','PSV','HRTX','HT','HT^C','HT^D','HT^E','HSY','HTZ','HSKA','HES','HESM','HPE','HXL','HX','HEXO','HFEN','HFFG','HIBB','PCF','CAPAU','HGLB','HFRO','HFRO^A','SNLN','HPK','HPKEW','HPR','HIHO','HIW','HIL','HI','HLM^','HRC','HTH','HGV','HLT','HIMX','HIFS','HQI','HSTO','HKIT','HCCH','HCCHR','HCCHU','HCCHW','HMG','HMNF','HMSY','HNI','HMLP','HMLP^A','HOLUU','HEP','HFC','HOLI','HOLX','HBCP','HOMB','HD','HFBL','HMST','HTBI','FIXX','HMC','HON','HOFT','HOOK','HOPE','HMN','HZAC.U','HBNC','HZN','HRZN','HTFA','HZNP','HRL','HST','TWNK','TWNKW','HOTH','HMHC','HLI','HUSA','HWCC','HOV','HOVNP','HBMD','HHC','HWM','HWM^','HPQ','HPX','HPX.U','HPX.WS','HSBC','HSBC^A','HTGM','HMI','HNP','HTHT','HUBG','HUBB','HUBS','HBM','HUSN','HEC','HECCU','HECCW','HSON','HUD','HPP','HDSN','HUIZ','HUM','HGEN','HCFT','HBAN','HBANN','HBANO','HII','HUN','HURC','HURN','HCM','HBP','HUYA','HVBC','H','HYMC','HYMCW','HYMCZ','HYRE','HY','IIIV','IAA','IAC','IAG','IBEX','IBIO','IBO','ICAD','IEP','ICCH','ICFI','ICHR','IBN','ICL','ICLK','ICLR','ICON','ICUI','IDA','IPWR','IDEX','IDYA','INVE','IDRA','IEX','IDXX','IDT','IEC','IESC','IROQ','IFMK','IGMS','IHRT','INFO','INFO','IIVI','IIVIP','IKNX','ITW','ILMN','IMAB','IMAC','IMACW','ISNS','IMRA','IMAX','IMBI','IMTX','IMTXW','IMMR','ICCC','IMUX','IMGN','IMMU','IMVT','IMRN','IMRNW','IMMP','IMH','IMO','PI','IMV','NARI','IOR','INCY','ICD','IHC','IRT','INDB','IBCP','IBTX','IFN','IGC','INDO','ILPT','ITACU','IBA','INFN','INFI','IFRX','III','INFY','IEA','IEAWW','INFU','ING','IR','NGVT','IMKTA','INGR','INBX','INMD','INMB','IPHA','INWK','INOD','IOSP','IIPR','IIPR^A','ISSC','INVA','IHT','INGN','INOV','INO','INZY','IPHI','INPX','INSG','NSIT','INSI','ISIG','INSM','NSP','INSP','INSE','NSPR','NSPR.WS','NSPR.WS.B','IBP','IIIN','INAQU','PODD','INSU','INSUU','INSUW','NTEC','ITGR','IART','ITRG','IMTE','INTC','NTLA','IDN','INS','IPAR','IBKR','ICPT','ICE','IHG','IFS','IDCC','TILE','IBOC','IBM','IFF','IFFT','IGT','IGIC','IGICW','IMXI','IP','INSW','INSW^A','THM','IDXG','IPV','IPV.U','IPV.WS','IPG','XENT','IPLDP','INTT','IVAC','ITCI','IPI','IIN','INTU','ISRG','INUV','IVC','IVA','PLW','VKI','ADRE','VBF','BSCK','BSJK','BSCL','BSJL','BSML','BSAE','BSCM','BSJM','BSMM','BSBE','BSCN','BSJN','BSMN','BSCE','BSCO','BSJO','BSMO','BSDE','BSCP','BSJP','BSMP','BSCQ','BSJQ','BSMQ','BSCR','BSJR','BSMR','BSCS','BSJS','BSMS','BSCT','BSMT','BSCU','BSMU','PKW','VCV','VTA','PFM','PYZ','PEZ','PSL','PIZ','PIE','PXI','PFI','PTH','PRN','PDP','DWAS','PTF','PUI','IDLB','PRFZ','PIO','PGJ','IHIT','IHTA','VLT','PEY','IPKW','PID','KBWB','KBWD','KBWY','KBWP','KBWR','IVR','IVR^B','IVR^C','IVR^A','OIA','VMO','VKQ','PNQI','PDBC','VPV','IVZ','QQQ','IQI','ISDX','ISEM','IUS','IUSS','USLB','PSCD','PSCC','PSCE','PSCF','PSCH','PSCI','PSCT','PSCM','PSCU','VVR','VTN','VGM','IIM','VRIG','PHO','ISTR','CMFNL','ICMB','ISBC','IRET','IRET^C','ITIC','NVTA','INVH','NVIV','IO','IONS','IOVA','IPGP','CLRG','CSML','IQ','IQV','IRMD','IRTC','IRIX','IRDM','IRBT','IRM','IRWD','IRS','IRCP','SLQD','ISHG','SHY','TLT','IEI','IEF','AIA','USIG','COMT','ISTB','IXUS','IUSG','IUSV','IUSB','HEWG','HYXF','DMXF','USXF','SUSB','ESGD','ESGE','ESGU','SUSC','LDEM','SUSL','XT','FALN','IFGL','BGRN','IGF','GNMA','IBTA','IBTB','IBTD','IBTE','IBTF','IBTG','IBTH','IBTI','IBTJ','IBTK','IGIB','IGOV','EMB','MBB','JKI','ACWX','ACWI','AAXJ','EWZS','MCHI','SCZ','EEMA','EMXC','EUFN','IEUS','RING','SDG','EWJE','EWJV','ENZL','QAT','TUR','UAE','IBB','SOXX','PFF','AMCA','EMIF','ICLN','WOOD','INDY','IJT','DVY','SHV','IGSB','ISR','ISDR','STAR          ','STAR^D','STAR^G','STAR^I','ITP','ITCB','ITMR','ITUB','ITOS','ITI','ITRM','ITRI','ITT','ITRN','ISEE','IVH','IZEA','JJSF','JPM','JPM^C','JPM^D','JPM^G','JPM^H','JPM^J','JAX','JILL','MAYS','JBHT','SJM','JCOM','JBL','JKHY','JACK','J','JAGX','JAKK','JHX','JRVR','JAMF','JAN','JHG','JSML','JSMD','JOF','JWS','JWS.U','JWS.WS','JAZZ','JBGS','JD','JEF','JELD','JRSH','JCAP','JCAP^B','JBLU','JCTCF','FROG','JT','JFIN','JKS','JMP','JMPNL','JMPNZ','JBSS','JBT','BTO','HEQ','JHS','JHI','HPF','HPI','HPS','PDT','HTD','HTY','JW.A','JW.B','JNJ','JCI','JOUT','JLL','JNCE','YY','JMIA','JIH','JIH.U','JIH.WS','JNPR','JP','JAQC','JAQCU','JE','JE^A','LRN','KAI','KDMN','KALU','KXIN','KALA','KLDO','KLR','KLR.WS','KALV','KMDA','KAMN','KNDI','KSU','KSU^','KAR','KRTX','KPTI','KSPN','KMF','KYN','KZIA','KB','KBH','KBLM','KBLMR','KBLMU','KBLMW','KBR','KBSF','BEKE','KRNY','K','KELYA','KELYB','KIQ','KMPR','KMT','KW','KEN','KCAC','KCAC.U','KCAC.WS','KFFB','KROS','KDP','KEQU','KTCC','KEY','KEY^I','KEY^J','KEY^K','KEYS','KZR','KFRC','KRC','KE','KBAL','KRP','KMB','KIM','KIM^L','KIM^M','KMI','KIN','KC','KINS','KFS','KNSA','KGC','KNSL','KTRA','KEX','KL','KIRK','KSMTU','KRG','KTOV','KTOVW','KKR','KKR^A','KKR^B','KKR^C','KIO','KREF','KLAC','KLXE','KNX','KNL','KNOP','KN','KOD','KSS','PHG','KTB','KOPN','KOP','KEP','KF','KFY','KRNT','KOS','KOSS','KWEB','KRA','KTOS','KR','KRO','KRYS','KT','KBNT','KBNTW','KLIC','KURA','KRUS','KVHI','KYMR','LB','FSTR','SCX','LHX','LJPC','LH','LADR','LAIX','LSBK','LBAI','LKFN','LAKE','LRCX','LAMR','LW','LANC','LCA','LCAHU','LCAHW','LNDC','LARK','LMRK','LMRKN','LMRKO','LMRKP','LE','LSTR','LCI','LTRN','LNTH','LTRX','LPI','LRMR','LVS','LSCC','LAUR','LAWS','LGI','LAZ','LZB','LAZY','LCII','LCNB','LEAF','LPTX','LEA','LEE','LGC','LGC.U','LGC.WS','LEGH','LEGN','INFR','LVHD','SQLV','LEG','JBK','KTH','KTN','LDOS','LACQ','LACQU','LACQW','LEJU','LMAT','LMND','LC','TREE','LEN','LEN.B','LII','LEVL','LEVLP','LEVI','LXRX','LX','LXP','LXP^C','LFAC','LFACU','LFACW','LPL','LGIH','LGL','DFNS','DFNS.U','DFNS.WS','LHCG','LI','LLIT','USA','ASG','LBRDA','LBRDK','LBTYA','LBTYB','LBTYK','LILA','LILAK','LILAR','BATRA','BATRK','FWONA','FWONK','LSXMA','LSXMB','LSXMK','LBRT','LTRPA','LTRPB','LSI','LSAC','LSACU','LSACW','LCUT','LFVN','LWAY','LGND','LTBR','LITB','LPTH','LSPD','LMB','LLNW','LMST','LMNL','LMNR','LINC','LECO','LNC','LIND','LIN','LNN','LN','LCTX','LINX','LGHL','LGHLW','LCAPU','LGF.A','LGF.B','LPCN','LIQT','YVR','LQDA','LQDT','LAD','LAC','LFUS','LIVK','LIVKU','LIVKW','LIVN','LYV','LOAK','LOAK.U','LOAK.WS','LOB','LIVE','LTHM','LPSN','RAMP','LIVX','LVGO','LIZI','LKQ','LYG','LMFA','LMFAW','LMPX','SCD','LMT','L','LOGC','LOGI','LOMA','CNCR','CHNA','LONE','LOAC','LOACR','LOACU','LOACW','LGVW','LGVW.U','LGVW.WS','LOOP','LORL','LPX','LOW','LPLA','LXU','LYTS','LTC','LUB','LULU','LL','LITE','LMNX','LUMO','LUNA','LKCO','LBC','LXFR','LDL','LYFT','LYB','LYRA','MTB','MDC','MHO','MCBC','MAC','CLI','MFNC','MTSI','MFD','MGU','MIC','BMA','MGNX','M','MCN','MSGE','MSGS','MDGL','MAG','MAGS','MGLN','MMP','MGTA','MGIC','MGA','MX','MGNI','MGY','MGYR','MH^A','MH^C','MH^D','MHLA','MHLD','MHNC','MAIN','MMD','MNSB','MNSBP','MJCO','MMYT','MLAC','MLACU','MLACW','MBUU','MNK','MLVF','TUSK','MANU','MANH','LOAN','MNTX','MTW','MTEX','MN','MNKD','MAN','MANT','MFC','MRO','MARA','MPC','MCHX','MMI','MCS','MRIN','MARPS','MPX','HZO','MRNS','MKL','MRKR','MKTX','MRLN','MAR','VAC','MBII','MMC','MRTN','MLM','MMLP','MRVL','MAS','MASI','DOOR','MTZ','MHH','MA','MCFT','MTDR','MTCH','MTLS','MTRN','MTNB','MTRX','MATX','MAT','MATW','MLP','MMX','MAXR','MAXN','MXIM','MMS','MXL','MEC','MBI','MKC','MKC.V','MCD','MUX','MGRC','MCK','MDCA','MDJH','MDU','MTL','MTL^','MDRR','MDRRP','MDLA','MBNKP','MFIN','MFINL','MDIA','MPW','MDNA','MNOV','MED','MDGS','MDGSW','MDWD','MCC','MCV','MCX','MDLQ','MDLX','MDLY','MD','MEDP','MDT','MFAC','MFAC.U','MFAC.WS','MEIP','MGTX','MLCO','MTSL','MELI','MBWM','MERC','MBIN','MBINO','MBINP','MRK','MFH','MCY','MRCY','MDP','MREO','MCMJ','MCMJW','EBSB','VIVO','MRBK','MMSI','MTH','MTOR','SNUG','MER^K','IPB','MACK','MRSN','MRUS','MESA','MLAB','MTR','MSB','MESO','CASH','MTCR','MTA','METX','METXW','MEOH','MEI','MET','MET^A','MET^E','MET^F','MCBS','MCB','MTD','MXC','MXE','MXF','MFA','MFA^B','MFA^C','MFO','MCR','MGF','MIN','MMT','MFM','MFV','MGEE','MTG','MGP','MGM','MGPI','MFGP','MBOT','MCHP','MU','MSFT','MSTR','MVIS','MICT','MPB','MAA','MAA^I','MTP','MCEP','MBCN','MSEX','MSBI','MSVB','AMPY','MOFG','MIST','MLSS','MLND','MLR','HIE','TIGO','MIME','MNDO','MIND','MINDP','MTX','NERV','MGEN','MRTX','MIRM','MSON','MG','MITK','MUFG','MIXT','MFG','MKSI','MMAC','MTC','MBT','MOBL','MODN','MRNA','MOD','MC','MOGO','MOGU','MWK','MHK','MKD','MTEM','MBRX','MOH','TAP','TAP.A','MNTA','MOMO','MKGI','MCRI','MDLZ','MGI','MDB','MNR','MNR^C','MNCL','MNCLU','MNCLW','MPWR','MNPR','MNRO','MRCC','MRCCL','MNST','MR','MEG','MCO','MOG.A','MOG.B','MS','MS^A','MS^E','MS^F','MS^I','MS^K','MS^L','CAF','MSD','EDD','IIF','MORN','MORF','MOR','MOS','MOSY','MPAA','MSI','MOTS','MCAC','MCACR','MCACU','MOV','MOXC','MPLX','COOP','MRC','MSA','MSM','MSCI','MSGN','MTBC','MTBCP','MTSC','MLI','MWA','MVF','MZA','MUR','MUSA','GRIL','MBIO','MVO','MVBF','MVC','MVCD','MYSZ','MYE','MYL','MYOK','MYO','MYOS','MYOV','MYRG','MYGN','NBR','NBR^A','NBRV','NC','NAKD','NTP','NNDM','NSTG','NAOV','NNVC','NNOX','NH','NK','NSSC','ATEST','ATEST.A','ATEST.B','ATEST.C','NTEST','NTEST.A','NTEST.B','NTEST.C','NDAQ','NTRA','NATH','NBHC','NKSH','FIZZ','NCMI','NESR','NESRW','NFG','NGHC','NGHCN','NGHCO','NGHCP','NGHCZ','NGG','NHI','NHC','NHLD','NHLDW','NATI','NOV','NPK','NRC','NNN','NNN^F','NRUC','NSEC','SID','NSA','NSA^A','EYE','NWLI','NTCO','NAII','NGS','NGVC','NHTC','NRP','NATR','NTUS','NTZ','NWG','NLS','NAVB','JSM','NAVI','NVGS','NNA','NMCI','NM','NM^G','NM^H','NMM','NAV','NAV^D','NSH.U','NBTB','NCNO','NCR','NCSM','NP','NKTR','NNI','NMRD','NEOG','NEO','NLTX','NEON','NPTN','NEOS','NVCN','NEPH','NEPT','NSCO','NSCO.WS','UEPS','NETE','NTAP','NTES','NFIN','NFINU','NFINW','NFLX','NTGR','NTCT','NTWK','NTST','NTIP','NBSE','NBW','NHS','NML','NBH','NBO','NRO','NRBO','NBIX','NURO','STIM','NTRP','NVRO','HYB','GBR','NEN','NFE','NFH','NFH.WS','GF','NGD','NWHM','IRL','NMFC','NMFCL','EDU','NPA','NPAUU','NPAWW','NEWR','NRZ','NRZ^A','NRZ^B','NRZ^C','SNR','NYC','NYCB','NYCB^A','NYCB^U','NYMT','NYMTM','NYMTN','NYMTO','NYMTP','NYT','NBEV','NEWA','NBAC','NBACR','NBACU','NBACW','NWL','NWGI','NHICU','NJR','NMRK','NEU','NEM','NR','NWS','NWSA','NEWT','NEWTI','NEWTL','NEXA','NXMD','NXE','NREF','NREF^A','NXRT','NHF','NXST','NXTC','NEXT','NEP','NEE','NEE^I','NEE^J','NEE^K','NEE^N','NEE^O','NEE^P','NXGN','NEX','NGL','NGL^B','NGL^C','NGM','NODK','NMK^B','NMK^C','EGOV','NICE','NICK','NCBS','NLSN','NKE','NKLA','NINE','NIO','NI','NI^B','NIU','NKTX','NL','LASR','NMIH','NNBR','NOAH','NBL','NBLX','NOK','NOMD','NMR','NDLS','OSB','NAT','NDSN','JWN','NSC','NSYS','NOA','NRT','NBN','NAK','NGA.U','NOG','NTIC','NTRS','NTRSO','NFBK','NRIM','NOC','NWBI','NWN','NWPX','NWE','NLOK','NCLH','NCLH','NWFL','NVFY','NVMI','NBY','NG','NOVN','NOVT','NVS','NVAX','NVO','NVCR','NOVS','NOVSU','NOVSW','NVUS','DNOW','NRG','NTN','NUS','NUAN','NCNA','NUE','NRIX','NS','NS^A','NS^B','NS^C','NSS','NTNX','NTR','NUVA','NVG','NUV','NUW','NEA','NAZ','NKX','NCB','NCA','NAC','JCE','JHAA','JHB','JCO','JQC','JDD','DIAX','NDMO','JEMD','NEV','JFR','JRO','NKG','JGH','JHY','NXC','NXN','NID','NMY','NMT','NUM','NMS','NOM','JLS','JMM','NHA','NZF','NMCO','NMZ','NMI','QQQX','NJV','NXJ','NRK','NYV','NNY','NAN','NUO','NPN','NQP']
            return list(set([item.split('.')[0].split('^')[0].strip() for item in fullList]))
        except: 'error'
    elif assets == 'SchwabETFs':
        return ['SCHK','SCHB','SCHX','SCHD','SCHM','SCHA','SCHG','SCHV','SCHH','FNDB','FNDX','FNDA','SCHF','SCHC','SCHE','FNDF','FNDC','FNDE','SCHI','SCHJ','SCHZ','SCHO','SCHR','SCHQ','SCHP']
    else:
        return ['CHIX', 'QQQC', 'SDEM','ABCDEFG']



def priceHist2PandasDF(symbol=None,beginning='1990-01-01',ending=None):
    '''
    Takes: asset symbol and (optionally) date range in form YYYY-MM-DD
    Returns: pandas dataframe
    TODO: beginning setting isn't working
    '''
    import logging
    logging.info('running priceHist2DF function')
    try:
        import yfinance as yf
        import datetime
    except:
        logging.critical('need yfinance and datetime modules.')
        return


    if type(beginning) is str:
        beginningSplit = beginning.split('-')
        beginning = datetime.datetime(int(beginningSplit[0]),int(beginningSplit[1]),int(beginningSplit[2]))
    elif type(beginning) is datetime.datetime:
        pass
    else:
        beginning = datetime.datetime(1990,1,1)

    if type(ending) is str:
        endingSplit = ending.split('-')
        ending = datetime.datetime(int(endingSplit[0]),int(endingSplit[1]),int(endingSplit[2]))
    elif type(ending) is datetime.datetime:
        pass
    else:
        ending = datetime.datetime.now()

    try:
        tickerData = yf.Ticker(symbol)
        result = tickerData.history(period='1d', start=beginning, end=ending)
        logging.info('getting data from yahoo.')
        result = result.drop('Dividends',axis=1)
        result = result.drop('Stock Splits',axis=1)
        result['Symbol']=symbol
        result['DateCol']=result.index
        #result = result.reset_index(level=['Date'])
    except:
        logging.warning('unable to retrieve data. check symbol.')
        result = None

    return result

def priceHist2SparkDF(symbol=None,beginning='1990-01-01',ending=None):
    '''
    Takes: asset symbol and (optionally) date range in form YYYY-MM-DD
    Returns: spark dataframe
    Assumes: sqlContext is already defined
    '''
    return sqlContext.createDataFrame(priceHist2PandasDF(symbol=symbol,beginning=beginning,ending=ending))


###########################
# preprocess
###########################

def elongate(df=None):
    '''
    Expects a pandas dataframe with 'Open', 'Close', 'DateCol', and 'Symbol' fields. Unions OPEN with CLOSE
    '''
    import pandas
    import datetime

    opens = df[['DateCol','Symbol','Open']]
    opens.columns = ['DateCol','symbol','price']
    opens['datetime'] = opens.loc[:,'DateCol'].apply(lambda d: datetime.datetime(d.year,d.month,d.day,9,30))
    opens.drop('DateCol', axis=1)

    closes = df[['DateCol','Symbol','Close']]
    closes.columns = ['DateCol','symbol','price']
    closes['datetime'] = closes.loc[:,'DateCol'].apply(lambda d: datetime.datetime(d.year,d.month,d.day,16,0))
    closes.drop('DateCol', axis=1)

    df = pandas.concat([opens,closes])
    return df[['datetime','symbol','price']].sort_values(['symbol','datetime']).reset_index(drop=True)

def window(df=None,windowCol='price',windowSize=5,dropNulls=False):
    '''
    Expects pandas dataframe sorted appropriately. Returns a windowed df
    '''
    tempDF = df.copy()
    for i in range(windowSize):
        tempDF[windowCol+'_t-'+str(i)] = tempDF[windowCol].shift(i)
    #tempDF = tempDF.drop('price',axis=1)
    #if dropNulls:
    #    tempDF = tempDF.dropna()
    return tempDF

def rowNormalize(df=None):
    '''
    Normalizes by row across all numeric values with smallest value mapped to 1
    '''
    tempDF = df.copy()
    numericCols = tempDF.select_dtypes(include=['floating','float64']).columns
    tempDF['min'] = tempDF.min(axis=1,numeric_only=True)
    tempDF['max'] = tempDF.max(axis=1,numeric_only=True)
    for col in numericCols:
    #    tempDF[col] = (tempDF[col] - tempDF['min']) / (tempDF['max'] - tempDF['min'])
        tempDF[col] = tempDF[col] / tempDF['min']
    return tempDF.drop(['min','max'],axis=1)

def preprocess(symbol='',beginning='1990-01-01',ending=None,windowSize=10,horizon=2,setLabel=True,dropNulls=True):
    df = priceHist2PandasDF(symbol=symbol,beginning=beginning,ending=ending)
    df = elongate(df)
    norm = rowNormalize(df=window(df=df,windowSize=windowSize,dropNulls=True))
    if setLabel:
        df['label'] = (df.price.shift(-horizon) - df.price.shift(-1)) / df.price.shift(-1)
        df = df.drop('price',axis=1)
    if dropNulls:
        df = df.dropna()
    return df.merge(norm,on=['datetime','symbol'],how='inner')

def inf2null(df=None):
    import numpy
    tempDF = df.copy()
    tempDF = tempDF.replace([numpy.inf, -numpy.inf], numpy.nan)
    return tempDF

###########################
# Create Labels
###########################
'''
def highPoint(df=None,horizon=7):

    #Expects a pandas dataframe in standard OHLCV format. Returns dataframe with new column 'highest'
    #TODO: currently this includes the current day...need to limit to sarting the next day.

    import logging
    logging.info('running highPoint function')
    try:
        import pandas
    except:
        logging.critical('need pandas and datetime modules.')
        return
    tempDF = df.copy()
    tempDF['highest'] = tempDF['High'].shift(-1)[::-1].rolling(window=horizon,center=False).max()
    return tempDF
'''

'''
def bracket(df=None, horizon=7):
    import logging
    logging.info('running percentChange')
    try:
        import pandas
    except:
        logging.critical('need pandas module.')
        return
    tempDF = df.copy()
    tempDF['nextDayOpen'] = tempDF.Open.shift(-1)
    tempDF['nextDayHigh'] = tempDF.High.shift(-1)
    tempDF['nextDayLow'] = tempDF.Low.shift(-1)
    for h in range(1,horizon+1):
'''

def percentChange(df=None, horizon=7, HighOrClose='High'):
    '''
    Expects a pandas dataframe in standard OHLCV format. Returns dataframe with new column 'percentChange'
    '''
    import logging
    logging.info('running percentChange')
    try:
        import pandas
    except:
        logging.critical('need pandas module.')
        return
    tempDF = df.copy()
    tempDF['nextDayOpen'] = tempDF.Open.shift(-1)
    #tempDF = highPoint(tempDF, horizon=horizon)
    tempDF['highest'] = tempDF[HighOrClose].shift(-1)[::-1].rolling(window=horizon,center=False).max()

    fieldName = 'lab_percentChange_H' + str(horizon) + HighOrClose
    tempDF[fieldName] = (tempDF['highest'] - tempDF['nextDayOpen']) / tempDF['nextDayOpen']
    return tempDF.drop(['highest','nextDayOpen'], 1)

def lowPercentChange(df=None, horizon=7):
    '''
    Expects a pandas dataframe in standard OHLCV format. Returns dataframe with new column 'lowPercentChange'
    '''
    import logging
    logging.info('running percentChange')
    try:
        import pandas
    except:
        logging.critical('need pandas module.')
        return
    tempDF = df.copy()
    tempDF['nextDayOpen'] = tempDF.Open.shift(-1)
    #tempDF = highPoint(tempDF, horizon=horizon)
    tempDF['lowest'] = tempDF['Low'].shift(-1)[::-1].rolling(window=horizon,center=False).min()

    fieldName = 'lab_lowPercentChange_H' + str(horizon)
    tempDF[fieldName] = (tempDF['lowest'] - tempDF['nextDayOpen']) / tempDF['nextDayOpen']
    return tempDF.drop(['lowest','nextDayOpen'], 1)

def buy(df=None, horizon=7, HighOrClose='High', threshold=0.01):
    '''
    Expects a pandas dataframe in standard OHLCV format. Returns dataframe with new column 'buy'
    '''
    tempDF = df.copy()
    tempDF = percentChange(tempDF, horizon=horizon, HighOrClose=HighOrClose)
    pctChangeFieldName = 'lab_percentChange_H' + str(horizon) + HighOrClose
    fieldName = 'lab_buy_H' + str(horizon) + HighOrClose + '_' + str(threshold)
    tempDF[fieldName] = tempDF[pctChangeFieldName] >= threshold
    return tempDF.drop([pctChangeFieldName], 1)

def absolutePercentChange(df=None, horizon=7, HighOrClose='High'):
    tempDF = df.copy()
    tempDF = percentChange(tempDF, horizon=horizon, HighOrClose=HighOrClose)
    pctChangeFieldName = 'lab_percentChange_H' + str(horizon) + HighOrClose
    fieldName = 'lab_absolutePercentChange_H' + str(horizon) + HighOrClose
    tempDF[fieldName] = tempDF[pctChangeFieldName].abs()
    return tempDF.drop([pctChangeFieldName], 1)

def expectPositiveReturn(df=None, horizon=7):
    '''
    Expects a pandas dataframe in standard OHLCV format. Returns dataframe with new column
    '''
    import logging
    logging.info('running percentChange')
    try:
        import pandas
        import numpy
    except:
        logging.critical('need pandas and numpy.')
        return
    tempDF = df.copy()
    tempDF['nextDayOpen'] = tempDF.Open.shift(-1)
    tempDF['highest'] = tempDF.High.shift(-1)[::-1].rolling(window=horizon,center=False).max()
    tempDF['lowest'] = tempDF.Low.shift(-1)[::-1].rolling(window=horizon,center=False).min()
    fieldName = 'lab_expectPositiveReturn_H{}'.format(horizon)
    tempDF[fieldName]=''
    for i in [j+1 for j in range(horizon)]:
        tempDF['ithDayhighest'] = tempDF.High.shift(-1)[::-1].rolling(window=i,center=False).max()
        tempDF['ithDaylowest'] = tempDF.Low.shift(-1)[::-1].rolling(window=i,center=False).min()
        tempDF[fieldName] = tempDF.apply(lambda x: False if x['lowest'] == x['ithDaylowest'] and x[fieldName]!=True else x[fieldName],axis=1)
        tempDF[fieldName] = tempDF.apply(lambda x: True if x['highest'] == x['ithDayhighest'] and x[fieldName]!=False else x[fieldName],axis=1)
    tempDF[fieldName] = tempDF.apply(lambda x: False if x[fieldName]!=True else x[fieldName],axis=1)
    return tempDF.drop(['nextDayOpen','highest','lowest','ithDayhighest','ithDaylowest'], 1)

def expectedReturnPct(df=None, horizon=7):
    '''
    Expects a pandas dataframe in standard OHLCV format. Returns dataframe with new column
    '''
    import logging
    logging.info('running percentChange')
    try:
        import pandas
        import numpy
        import math
    except:
        logging.critical('need pandas and numpy.')
        return
    tempDF = df.copy()
    tempDF['nextDayOpen'] = tempDF.Open.shift(-1)
    tempDF['highest'] = tempDF.High.shift(-1)[::-1].rolling(window=horizon,center=False).max()
    tempDF['lowest'] = tempDF.Low.shift(-1)[::-1].rolling(window=horizon,center=False).min()
    tempDF = tempDF.dropna()
    fieldName = 'lab_expectedReturnPct_H{}'.format(horizon)
    tempDF[fieldName]=''
    for i in [j+1 for j in range(horizon)]:
        tempDF['ithDayhighest'] = tempDF.High.shift(-1)[::-1].rolling(window=i,center=False).max()
        tempDF['ithDaylowest'] = tempDF.Low.shift(-1)[::-1].rolling(window=i,center=False).min()
        tempDF[fieldName] = tempDF.apply(lambda x: False if x['lowest'] == x['ithDaylowest'] and x[fieldName]!=True else x[fieldName],axis=1)
        tempDF[fieldName] = tempDF.apply(lambda x: True if x['highest'] == x['ithDayhighest'] and x[fieldName]!=False else x[fieldName],axis=1)
    tempDF[fieldName] = tempDF.apply(lambda x: False if x[fieldName]!=True else x[fieldName],axis=1)
    tempDF[fieldName] = tempDF.apply(lambda x: 'n'+str(math.ceil(100.000000000000*(x['lowest']-x['nextDayOpen'])/x['nextDayOpen'])) if x[fieldName]==False else 'p'+str(math.floor(100.000000000000*(x['highest']-x['nextDayOpen'])/x['nextDayOpen'])),axis=1)
    return tempDF.drop(['nextDayOpen','highest','lowest','ithDayhighest','ithDaylowest'], 1)

def weights(df=None, horizon=7,weightForIncrease=1,weightForDecrease=2):
    '''
    Expects a pandas dataframe in standard OHLCV format. Returns dataframe with new column
    '''
    tempDF = df.copy()
    tempDF['nextDayOpen'] = tempDF.Open.shift(-1)
    tempDF['finalDayClose'] = tempDF.Close.shift(-1*horizon)
    tempDF['weights'] = tempDF.apply(lambda x: weightForIncrease if x['finalDayClose']>x['nextDayOpen'] else weightForDecrease,axis=1)
    return tempDF.drop(['nextDayOpen','finalDayClose'], 1)



###########################
# Create Features
###########################


def DOW(df=None,dateCol=None):
    tempDF = df.copy()
    newColName = 'feat_DOW'
    tempDF[newColName] = tempDF[dateCol].dt.dayofweek+1
    return tempDF

def SMA(df=None,colToAvg=None,windowSize=10):
    '''
    Expects a pandas dataframe df sorted in ascending order. Returns df with additional SMA column.
    Good for average price (whether just Closed or elongated Open+Close) and average volume.
    '''
    import pandas
    tempDF = df.copy()
    #return pandas.rolling_mean(tempDF[colToAvg],window=windowSize)  # deprecated to the below
    return tempDF[colToAvg].rolling(window=windowSize,center=False).mean()

def SMARatio(df=None,colToAvg=None,windowSize1=10,windowSize2=20):
    tempDF = df.copy()
    newColName = 'feat_' + colToAvg+str(windowSize1)+'/'+str(windowSize2)+'SMARatio'
    firstSMA = SMA(df=df,colToAvg=colToAvg,windowSize=windowSize1)
    secondSMA = SMA(df=df,colToAvg=colToAvg,windowSize=windowSize2)
    tempDF[newColName] = (firstSMA - secondSMA) / (firstSMA + secondSMA)
    #tempDF['feat_'+colToAvg+'DistFrom'+str(windowSize1)+'SMA'] = (tempDF[colToAvg] - firstSMA) / firstSMA
    return tempDF

def PctFromSMA(df=None,colToAvg=None,windowSize=10):
    tempDF = df.copy()
    newColName = 'feat_' + colToAvg+str(windowSize)+'PctFromSMA'
    SMAvg = SMA(df=df,colToAvg=colToAvg,windowSize=windowSize)
    tempDF[newColName] = (tempDF[colToAvg] - SMAvg) / SMAvg
    return tempDF

def CommodityChannelIndex(df=None, windowSize=10):
    tempDF = df.copy()
    newColName = 'feat_' + str(windowSize)+'CCI'
    TypicalPrice = (tempDF['High'] + tempDF['Low'] + tempDF['Close']) / 3
    RollingMean = TypicalPrice.rolling(window=windowSize,center=False).mean()
    RollingStd = TypicalPrice.rolling(window=windowSize,center=False).std()
    tempDF[newColName] = (TypicalPrice - RollingMean) / (0.015 * RollingStd)
    return tempDF

def EaseOfMovement(df=None, windowSize=10):
    tempDF = df.copy()
    newColName = 'feat_' + str(windowSize)+'EMV'
    dm = ((tempDF['High'] + tempDF['Low'])/2) - ((tempDF['High'].shift(1) + tempDF['Low'].shift(1))/2)
    avgVol = tempDF['Volume'].rolling(window=windowSize,center=False).mean()
    br = (tempDF['Volume'] / avgVol) / ((tempDF['High'] - tempDF['Low'])) # typically have vol/100000000 but changed to account for differences of average volume
    tempDF[newColName] = (dm / br).rolling(window=windowSize,center=False).mean()
    return tempDF

def ForceIndex(df=None, windowSize=10):
    import pandas
    tempDF = df.copy()
    newColName = 'feat_' + str(windowSize)+'FI'
    priceChange = (tempDF['High'] - tempDF['Open'])/tempDF['Open']
    avgVol = tempDF['Volume'].rolling(window=windowSize,center=False).mean()
    #pandas.ewm(priceChange*(tempDF['Volume']/avgVol),span=windowSize).mean()
    s=priceChange*(tempDF['Volume']/avgVol)
    tempDF[newColName] = s.ewm(ignore_na=False,span=windowSize,min_periods=windowSize,adjust=True).mean()
    return tempDF

def BollingerBands(df=None, colToAvg=None, windowSize=10):
    tempDF = df.copy()
    MA = tempDF[colToAvg].rolling(window=windowSize,center=False).mean()
    SD = tempDF[colToAvg].rolling(window=windowSize,center=False).std()
    UpperBB = MA + (2 * SD)
    LowerBB = MA - (2 * SD)
    tempDF['feat_' + colToAvg+str(windowSize)+'PctFromUpperBB'] = (tempDF[colToAvg] - UpperBB) / UpperBB
    tempDF['feat_' + colToAvg+str(windowSize)+'PctFromLowerBB'] = (tempDF[colToAvg] - LowerBB) / LowerBB
    tempDF['feat_' + colToAvg+str(windowSize)+'BBBandwidth'] = ( (UpperBB - LowerBB) / MA) * 100
    tempDF['feat_' + colToAvg+str(windowSize)+'PctB'] = (tempDF[colToAvg] - LowerBB)/(UpperBB - LowerBB)
    return tempDF

def PROC(df=None, colToAvg=None,windowSize=10):
    '''
    takes date-sorted dataframe with Volume column. returns dataframe with VROC column
    '''
    tempDF = df.copy()
    tempDF['priorPrice'] = tempDF[colToAvg].shift(windowSize)
    tempDF['feat_PROC'+str(windowSize)] = (tempDF[colToAvg] - tempDF['priorPrice']) / tempDF['priorPrice']
    tempDF.drop('priorPrice',inplace=True, axis=1)
    return tempDF

def VROC(df=None,windowSize=10):
    '''
    takes date-sorted dataframe with Volume column. returns dataframe with VROC column
    '''
    tempDF = df.copy()
    tempDF['priorVolume'] = tempDF['Volume'].shift(windowSize)
    tempDF['feat_VROC'+str(windowSize)] = (tempDF['Volume'] - tempDF['priorVolume']) / tempDF['priorVolume']
    tempDF.drop('priorVolume',inplace=True, axis=1)
    return tempDF

def autocorrelation(df=None,windowSize=10,colToAvg=None,lag=1):
    tempDF = df.copy()
    tempDF['feat_autocorr'+str(windowSize)+colToAvg+str(lag)] = tempDF[colToAvg].rolling(window=windowSize).corr(other=tempDF[colToAvg].shift(lag))
    return tempDF

def RSI(df=None,priceCol='Close',windowSize=14):
    '''
    takes date-sorted dataframe with price column (Close or price)
    '''
    import pandas
    tempDF = df.copy()
    tempDF['rsiChange']=tempDF[priceCol] - tempDF[priceCol].shift(1)
    tempDF['rsiGain']=tempDF['rsiChange'].apply(lambda x: 1.00000000*x if x>0 else 0)
    tempDF['rsiLoss']=tempDF['rsiChange'].apply(lambda x: 1.00000000*x if x<0 else 0).abs()
    tempDF['rsiAvgGain']=tempDF['rsiGain'].rolling(window=windowSize,center=False).sum() / windowSize
    tempDF['rsiAvgLoss']=tempDF['rsiLoss'].rolling(window=windowSize,center=False).sum() / windowSize
    tempDF['feat_RSI'+str(windowSize)] = tempDF.apply(lambda x: 100.00000000 if (x.rsiAvgLoss<0.00000000001 or x.rsiAvgLoss>-0.00000000001) else 0.00000000 if (x.rsiAvgGain<0.00000000001 or x.rsiAvgGain>-0.00000000001) else 100.00000000-(100.00000000/(1+(x.rsiAvgGain/x.rsiAvgLoss))),axis=1)
    tempDF.drop(['rsiChange','rsiGain','rsiLoss','rsiAvgGain','rsiAvgLoss'],inplace=True,axis=1)
    return tempDF


def RSIgranular(df=None,windowSize=14):
    '''
    uses elongated data
    '''
    import pandas
    window = 2 * windowSize
    tempDF = df.copy()
    tempDF = RSI(elongate(tempDF),priceCol='price',windowSize=window)
    tempDF.rename(columns={'feat_RSI'+str(window): 'feat_RSIg'+str(windowSize),'symbol':'Symbol'}, inplace=True)
    tempDF = tempDF[tempDF['datetime'].dt.hour == 16]
    tempDF['DateCol'] = tempDF['datetime'].dt.normalize()
    tempDF.drop(['datetime','price'],inplace=True,axis=1)
    tempDF2 = df.copy()
    return tempDF2.merge(tempDF,on=['Symbol','DateCol'], how='left')

def DMI(df=None,windowSize=14):
    '''
    Assumes df has High, Low, and Close columns
    '''
    import pandas
    tempDF = df.copy()
    tempDF['upMove'] = tempDF['High'] - tempDF['High'].shift(1)
    tempDF['downMove'] = tempDF['Low'].shift(1) - tempDF['Low']

    tempDF['posDM'] = tempDF.apply(lambda x: x['upMove'] if x['upMove'] > x['downMove'] and x['upMove']>0 else 0, axis=1)
    tempDF['negDM'] = tempDF.apply(lambda x: x['downMove'] if x['downMove'] > x['upMove'] and x['downMove']>0 else 0, axis=1)

    tempDF['high-low'] = tempDF['High'] - tempDF['Low']
    tempDF['high-close'] = abs(tempDF['High'] - tempDF['Close'].shift(1))
    tempDF['low-close'] = abs(tempDF['Low'] - tempDF['Close'].shift(1))
    tempDF['TR'] = tempDF[['high-low','high-close','low-close']].max(axis=1)

    tempDF['posDI'] = (tempDF['posDM'] / tempDF['TR']).ewm(span=windowSize).mean()
    tempDF['negDI'] = (tempDF['negDM'] / tempDF['TR']).ewm(span=windowSize).mean()


    tempDF['feat_DIRatio'+ str(windowSize)] = (100 * (tempDF['posDI'] - tempDF['negDI']) / (tempDF['posDI'] + tempDF['negDI'])).ewm(span=windowSize).mean()
    tempDF['feat_ADX'+ str(windowSize)] = (100 * abs(tempDF['posDI'] - tempDF['negDI']) / (tempDF['posDI'] + tempDF['negDI'])).ewm(span=windowSize).mean()
    tempDF['feat_DMI'+ str(windowSize)] = (tempDF['feat_ADX'+ str(windowSize)] * tempDF['feat_DIRatio'+ str(windowSize)]) / 100
    tempDF['feat_AdjATR'+ str(windowSize)] = (tempDF['TR']/tempDF['High']).rolling(window=windowSize,center=False).mean()

    tempDF = tempDF.drop(['upMove','downMove','posDM','negDM','high-low','high-close','low-close','TR','posDI','negDI'],axis=1)
    return tempDF


def MACD(df=None,colToAvg=None,windowSizes=[9,12,26]):
    import pandas
    tempDF = df.copy()
    newColName = 'feat_' + colToAvg+str(windowSizes[0])+str(windowSizes[1])+str(windowSizes[2])+'MACD'
    secondEMA = df[colToAvg].ewm(span=windowSizes[1]).mean()
    thirdEMA = df[colToAvg].ewm(span=windowSizes[2]).mean()
    MACD = (secondEMA - thirdEMA)
    signal = MACD.ewm(span=windowSizes[0]).mean()
    tempDF[newColName] = (MACD - signal) / (MACD + signal)
    return tempDF

def MACDgranular(df=None,windowSizes=[9,12,26]):
    '''
    uses elongated data
    '''
    import pandas
    frst = 2 * windowSizes[0]
    scnd = 2 * windowSizes[1]
    thrd = 2 * windowSizes[2]
    tempDF = MACD(elongate(df),colToAvg='price',windowSizes=[frst,scnd,thrd])

    tempDF.rename(columns={'feat_price' +str(frst)+str(scnd)+str(thrd)+'MACD': 'feat_' +str(windowSizes[0])+str(windowSizes[1])+str(windowSizes[2])+'MACDg','symbol':'Symbol'}, inplace=True)

    tempDF = tempDF[tempDF['datetime'].dt.hour == 16]
    tempDF['DateCol'] = tempDF['datetime'].dt.normalize()
    tempDF.drop(['datetime','price'],inplace=True,axis=1)
    tempDF2 = df.copy()
    return tempDF2.merge(tempDF,on=['Symbol','DateCol'], how='left')


def StochasticOscillator(df=None,windowSize=14):
    import pandas
    tempDF = df.copy()
    newColName = 'feat_' + str(windowSize)+'StocOsc'
    lowestLow = tempDF['Low'].rolling(window=windowSize,center=False).min()
    highestHigh = tempDF['High'].rolling(window=windowSize,center=False).max()
    K = (tempDF['Close'] - lowestLow) / (highestHigh - lowestLow) * 100
    D = K.rolling(window=3,center=False).mean()
    tempDF[newColName] = K
    tempDF[newColName+'Ratio'] = (K-D) / D
    return tempDF


def PriceChannels(df=None,windowSize=14):
    import pandas
    tempDF = df.copy()
    newColName = 'feat_' + str(windowSize)+'PriceChannelDist'
    lowestLow = tempDF['Low'].rolling(window=windowSize,center=False).min()
    highestHigh = tempDF['High'].rolling(window=windowSize,center=False).max()
    centerLine = (lowestLow + highestHigh) / 2
    tempDF[newColName] = (tempDF['Close'] - centerLine) / centerLine
    return tempDF


def PSAR(df=None, iaf = 0.02, maxaf = 0.2):
    import pandas
    barsdata = df.copy()
    length = len(barsdata)
    dates = list(barsdata['DateCol'])
    high = list(barsdata['High'])
    low = list(barsdata['Low'])
    close = list(barsdata['Close'])
    psar = close[0:len(close)]
    psarbull = [None] * length
    psarbear = [None] * length
    bull = True
    af = iaf
    ep = low[0]
    hp = high[0]
    lp = low[0]
    for i in range(2,length):
        if bull:
            psar[i] = psar[i - 1] + af * (hp - psar[i - 1])
        else:
            psar[i] = psar[i - 1] + af * (lp - psar[i - 1])
        reverse = False
        if bull:
            if low[i] < psar[i]:
                bull = False
                reverse = True
                psar[i] = hp
                lp = low[i]
                af = iaf
        else:
            if high[i] > psar[i]:
                bull = True
                reverse = True
                psar[i] = lp
                hp = high[i]
                af = iaf
        if not reverse:
            if bull:
                if high[i] > hp:
                    hp = high[i]
                    af = min(af + iaf, maxaf)
                if low[i - 1] < psar[i]:
                    psar[i] = low[i - 1]
                if low[i - 2] < psar[i]:
                    psar[i] = low[i - 2]
            else:
                if low[i] < lp:
                    lp = low[i]
                    af = min(af + iaf, maxaf)
                if high[i - 1] > psar[i]:
                    psar[i] = high[i - 1]
                if high[i - 2] > psar[i]:
                    psar[i] = high[i - 2]
        if bull:
            psarbull[i] = psar[i]
        else:
            psarbear[i] = psar[i]
    dictForDF = {'close':close,"psar":psar, "psarbear":psarbear, "psarbull":psarbull}
    newDF = pandas.DataFrame(dictForDF)
    newDF['dist'] = (newDF['close'] - newDF['psar']) / newDF['psar']
    tempDF = df.copy()
    tempDF['feat_PSAR'] = newDF['dist']
    return tempDF

def AccumulationDistributionLine(df=None,windowSize=10):
    import pandas
    tempDF = df.copy()
    newColName = 'feat_' + str(windowSize)+'ADL'
    MoneyFlowMultiplier = ((tempDF['Close']  -  tempDF['Low']) - (tempDF['High'] - tempDF['Close'])) /(tempDF['High'] - tempDF['Low'])
    RelativeVolume = tempDF['Volume'] / tempDF['Volume'].rolling(window=windowSize,center=False).sum()
    AdjMoneyFlowVolume = MoneyFlowMultiplier * RelativeVolume
    MoneyFlowVolume = MoneyFlowMultiplier * tempDF['Volume']
    tempDF[newColName] = AdjMoneyFlowVolume.rolling(window=windowSize,center=False).sum()
    tempDF['feat_' + str(windowSize)+'CMF'] = MoneyFlowVolume.rolling(window=windowSize,center=False).sum() / tempDF['Volume'].rolling(window=windowSize,center=False).sum()
    tempDF['feat_' + str(windowSize)+'ChaikinOscillator'] = tempDF[newColName].ewm(span=max(1,int(windowSize/3))).mean() - tempDF[newColName].ewm(span=windowSize).mean()
    return tempDF

def Aroon(df=None,windowSize=10):
    import numpy
    tempDF = df.copy()
    rmlagmax = lambda xs: numpy.argmax(xs[::-1])
    DaysSinceHigh = 1.00000000000*tempDF['High'].rolling(center=False,min_periods=windowSize,window=windowSize).apply(func=rmlagmax)
    rmlagmin = lambda xs: numpy.argmin(xs[::-1])
    DaysSinceLow = 1.00000000000*tempDF['Low'].rolling(center=False,min_periods=windowSize,window=windowSize).apply(func=rmlagmin)
    tempDF['feat_' + str(windowSize)+'AroonUp'] = ((windowSize - DaysSinceHigh*1.000000000)/windowSize) * 100.00000000000
    tempDF['feat_' + str(windowSize)+'AroonDown'] = ((windowSize - DaysSinceLow*1.000000000)/windowSize) * 100.00000000000
    tempDF['feat_' + str(windowSize)+'AroonOscillator'] = tempDF['feat_' + str(windowSize)+'AroonUp'] - tempDF['feat_' + str(windowSize)+'AroonDown']
    return tempDF

###########################
# Create Models
###########################



def OLD_run_model(df, model, WhichLabel, Resultant_Col_Name, sample_rate, incl_preds=False):
    '''
    Takes pandas dataframe, model, etc.
    TODO: decide if this should even be used
    TODO: investigate 'truncate_after' usage
    TODO: clean up reference to PricesPanel, etc.
    '''
    if incl_preds==True:
        PredCols = df.filter(regex="Pred_").columns.values
    else:
        PredCols = []
    LabelCols = df.filter(regex="Label_").columns.values
    xbase = df.copy().dropna().drop(LabelCols,axis=1).drop(PredCols,axis=1).truncate(after=truncate_after)
    sample_size = int(sample_rate*len(xbase.index))
    xtrain = xbase.ix[sorted(np.random.choice(xbase.index,sample_size))]
    ytrain = df.copy()[WhichLabel].ix[sorted(xtrain.index)]
    model.fit(xtrain,ytrain)
    to_predict = df.copy().drop(LabelCols,axis=1).dropna().drop(PredCols,axis=1)
    rslt = model.predict(to_predict)
    result = pd.DataFrame(rslt,index=to_predict.index, columns=[Resultant_Col_Name])
    dummy = PricesPanel[asset].join(result,how='left')
    PricesPanel[asset][Resultant_Col_Name] = dummy[Resultant_Col_Name]









###################################
