import urllib.request
import json
import csv
import time

key = "c137e42a99ef99f0"
base = open('americanToad.csv', 'rt')
out = open("americanToadCorre.csv","a")
reader = csv.reader(base)
i = 0
erros = []
fieldnames = ['date','utcdate','tempm', 'tempi', 'dewptm', 'dewpti', 'hum', 'wspdm', 'wspdi', 'wgustm', 'wgusti', 'wdird', 'wdire', 'vism', 'visi', 'pressurem', 'pressurei','windchillm', 'windchilli', 'heatindexm', 'heatindexi', 'precipm', 'precipi', 'conds', 'icon', 'fog', 'rain', 'snow', 'hail', 'thunder', 'tornado', 'metar']
writer = csv.DictWriter(out, fieldnames=fieldnames, delimiter = ',')
#writer.writeheader()
print("Iniciando coleta de dados...")
for row in reader:
    print(i,"resposta(s)")
    try:
        f = urllib.request.urlopen("http://api.wunderground.com/api/"+key+"/history_"+row[1]+"/q/"+row[2]+","+row[3]+".json")
        parsed_json = json.loads(f.read())
        writer.writerow(parsed_json['history']['observations'][0])
        f.close()
        i = i + 1
    except:
        print("Erro ao conectar API WU para o registro",row[0])
        erros.append(row[0])
        print("Registros com erro:",erros)
    if (i % 8 == 0 and i != 0):
        print("Aguardando 1 minuto...")
        time.sleep(60)
        print("Continuando coleta de dados...")

out.close()
base.close()
