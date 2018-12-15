import urllib.request
import json
import csv
import time

key = "c137e42a99ef99f0"
#base = open('americanToad.csv',"rt")
#out = open("americanToadCorre.csv","a")
base = open('springPeeper.csv',"rt")
out = open("springPeeperCorre.csv","w")
#base = open('greenFrog.csv',"rt")
#out = open("greenFrogCorre.csv","w")
reader = csv.reader(base)
i = 0
a = 0
erros = []
fieldnames = ['id','date','lat', 'lng', 'tornado', 'monthtodatesnowfalli', 'thunder', 'meantempi', 'snow', 'meanvism', 'meanwdire', 'minhumidity', 'maxvism', 'since1jancoolingdegreedays', 'coolingdegreedays', 'maxdewptm', 'monthtodatecoolingdegreedays', 'since1sepheatingdegreedays', 'precipi', 'heatingdegreedays', 'meanpressurem', 'since1julsnowfallm', 'monthtodatesnowfallm', 'precipsource', 'rain', 'minwspdm', 'since1sepcoolingdegreedaysnormal', 'meanwindspdi', 'monthtodateheatingdegreedaysnormal', 'snowfallm', 'fog', 'humidity', 'minwspdi', 'meanpressurei', 'gdegreedays', 'since1julheatingdegreedays', 'mindewptm', 'maxwspdi', 'minpressurem', 'coolingdegreedaysnormal', 'maxtempm', 'minvisi', 'meanwindspdm', 'mintempi', 'maxpressurem', 'since1sepheatingdegreedaysnormal', 'hail', 'meandewptm', 'maxwspdm', 'precipm', 'meantempm', 'since1julheatingdegreedaysnormal', 'snowfalli', 'monthtodateheatingdegreedays', 'monthtodatecoolingdegreedaysnormal', 'since1sepcoolingdegreedays', 'maxpressurei', 'minvism', 'minpressurei', 'maxvisi', 'heatingdegreedaysnormal', 'since1jancoolingdegreedaysnormal', 'snowdepthm', 'meanwdird', 'meandewpti', 'meanvisi', 'maxhumidity', 'snowdepthi', 'mintempm', 'since1julsnowfalli', 'maxtempi', 'maxdewpti', 'mindewpti']

writer = csv.DictWriter(out, fieldnames=fieldnames, delimiter = ',')
writerError = csv.writer(out)
writer.writeheader()
print("Iniciando coleta de dados...")
for row in reader:
    while (a < 3):
        try:
            f = urllib.request.urlopen("http://api.wunderground.com/api/"+key+"/history_"+row[1]+"/q/"+row[2]+","+row[3]+".json")
            parsed_json = json.loads(f.read())
            if ("history" in parsed_json and 'dailysummary' in parsed_json['history'] and len(parsed_json['history']['dailysummary']) > 0 ):
                parsed_json['history']['dailysummary'][0]['id'] = row[0]
                parsed_json['history']['dailysummary'][0]['date'] = row[1]
                parsed_json['history']['dailysummary'][0]['lat'] = row[2]
                parsed_json['history']['dailysummary'][0]['lng'] = row[3]
                writer.writerow(parsed_json['history']['dailysummary'][0])
            else:
                writerError.writerow([row[0],row[1],row[2],row[3]])
            f.close()
            a = 4
        except:
            a = a + 1
            print("["+ a +"] Falha ao coletar dados do registro "+row[0]+". Aguardando 10s para reconexão...")
            time.sleep(10)
            print("Reconectando API para registro "+row[0]+"...")
    if (a == 3):
        print("Erro ao conectar API WU para o registro",row[0])
        erros.append(row[0])
        print("Registros com erro:",erros)
    else:
        i = i + 1
        print(i,"resposta(s)")
    a = 0
    if (i % 9 == 0 and i != 0):
        print("Limite de 10/min atingido. Aguardando tempo necessário até próxima consulta...")
        for n in range(6, 0, -1):
            print(str(n)+"0 segundos restantes...")
            time.sleep(10)
        print("Continuando coleta de dados...")
    elif (i % 499 == 0 and i != 0):
        print("Limite de 500/dia atingido. Aguardando tempo necessário até próxima consulta...")
        for n in range(24, 0, -1):
            print(str(n),"hora(s) restante(s)...")
            time.sleep(3600)
        print("Continuando coleta de dados...")
print("Consulta finalizada!")
print("IDs com erro na consulta:")
print(erros)
out.close()
base.close()
