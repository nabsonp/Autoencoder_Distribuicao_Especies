import urllib.request
import json
import csv
import time

key = "c137e42a99ef99f0"
base = open('americanToad.csv', 'rt')
out = open("americanToadCorre.csv","a")
reader = csv.reader(base)
i = 0
a = 0
erros = []
fieldnames = ['date','lat','lng','utcdate','tempm', 'tempi', 'dewptm', 'dewpti', 'hum', 'wspdm', 'wspdi', 'wgustm', 'wgusti', 'wdird', 'wdire', 'vism', 'visi', 'pressurem', 'pressurei','windchillm', 'windchilli', 'heatindexm', 'heatindexi', 'precipm', 'precipi', 'conds', 'icon', 'fog', 'rain', 'snow', 'hail', 'thunder', 'tornado', 'metar']
writer = csv.DictWriter(out, fieldnames=fieldnames, delimiter = ',')
#writer.writeheader()
print("Iniciando coleta de dados...")
for row in reader:
    while (a < 3):
        try:
            f = urllib.request.urlopen("http://api.wunderground.com/api/"+key+"/history_"+row[1]+"/q/"+row[2]+","+row[3]+".json")
            parsed_json = json.loads(f.read())
            parsed_json['history']['observations'][0]['date'] = row[1]
            parsed_json['history']['observations'][0]['lat'] = row[2]
            parsed_json['history']['observations'][0]['lng'] = row[3]
            parsed_json['history']['observations'][0].pop("utcdate")
            parsed_json['history']['observations'][0].pop("metar")
            writer.writerow(parsed_json['history']['observations'][0])
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
    if (i % 8 == 0 and i != 0):
        print("Aguardando 1 minuto...")
        time.sleep(60)
        print("Continuando coleta de dados...")

out.close()
base.close()
