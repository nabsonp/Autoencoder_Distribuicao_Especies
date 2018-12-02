import urllib.request
import json
import csv
import time

key = "c137e42a99ef99f0"
base = open('americanToad.csv',"rt")
out = open("americanToadCorre.csv","w")
reader = csv.reader(base)
i = 0
a = 0
erros = []
fieldnames = ['date','lat', 'lng', 'tornado', 'monthtodatesnowfalli', 'thunder', 'meantempi', 'snow', 'meanvism', 'meanwdire', 'minhumidity', 'maxvism', 'since1jancoolingdegreedays', 'coolingdegreedays', 'maxdewptm', 'monthtodatecoolingdegreedays', 'since1sepheatingdegreedays', 'precipi', 'heatingdegreedays', 'meanpressurem', 'since1julsnowfallm', 'monthtodatesnowfallm', 'precipsource', 'rain', 'minwspdm', 'since1sepcoolingdegreedaysnormal', 'meanwindspdi', 'monthtodateheatingdegreedaysnormal', 'snowfallm', 'fog', 'humidity', 'minwspdi', 'meanpressurei', 'gdegreedays', 'since1julheatingdegreedays', 'mindewptm', 'maxwspdi', 'minpressurem', 'coolingdegreedaysnormal', 'maxtempm', 'minvisi', 'meanwindspdm', 'mintempi', 'maxpressurem', 'since1sepheatingdegreedaysnormal', 'hail', 'meandewptm', 'maxwspdm', 'precipm', 'meantempm', 'since1julheatingdegreedaysnormal', 'snowfalli', 'monthtodateheatingdegreedays', 'monthtodatecoolingdegreedaysnormal', 'since1sepcoolingdegreedays', 'maxpressurei', 'minvism', 'minpressurei', 'maxvisi', 'heatingdegreedaysnormal', 'since1jancoolingdegreedaysnormal', 'snowdepthm', 'meanwdird', 'meandewpti', 'meanvisi', 'maxhumidity', 'snowdepthi', 'mintempm', 'since1julsnowfalli', 'maxtempi', 'maxdewpti', 'mindewpti']

writer = csv.DictWriter(out, fieldnames=fieldnames, delimiter = ',')
writer.writeheader()
print("Iniciando coleta de dados...")
for row in reader:
    while (a < 3):
        try:
            f = urllib.request.urlopen("http://api.wunderground.com/api/"+key+"/history_"+row[1]+"/q/"+row[2]+","+row[3]+".json")
            parsed_json = json.loads(f.read())
            parsed_json['history']['dailysummary'][0]['date'] = row[1]
            parsed_json['history']['dailysummary'][0]['lat'] = row[2]
            parsed_json['history']['dailysummary'][0]['lng'] = row[3]
            writer.writerow(parsed_json['history']['dailysummary'][0])
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
        print("Aguardando 1 minuto...")
        time.sleep(60)
        print("Continuando coleta de dados...")

out.close()
base.close()
