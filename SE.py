from tokenize import triple_quoted
import requests 
import pandas as pd
pd.options.mode.chained_assignment = None
import time
import dataframe_image as dfi
from PIL import Image
from datetime import datetime
from cim_to_id import cim_to_id as cmm
import random

def format_float(value):
    return f'{value:,.2f}'

pd.options.display.float_format = format_float

currentDateAndTime = datetime.now()
currentTime = str(currentDateAndTime.strftime("%H:%M"))
hora = int(currentTime[0:2])
if hora <= 13:
    ventana = "1"
    cual = "## PRIMERA VENTANA ##"
elif hora >= 14:
    ventana = "2"
    cual = "## SEGUNDA VENTANA ##"


print("La hora actual es ",currentTime)
print("\n")
print(cual)
print("\n")

#ventana = input("1. Primera Ventana\n2.Segunda ventana\nElija la opcion: ")

### CREAR TOKEN Y PASARLO ###

now = str(datetime.now())
filename = ("Extracciones-"+now[0:10]+".png")
token = str(random.randint(111111111,99999999999))
print(f"Tu token es: {token}")
time.sleep(1)
print("Espere un segundo...")
time.sleep(0.5)

### PRIMERA CARGA: GENERAL 
### ESTO CARGA TODOS LOS ALYC CON SE


URL_GENERAL = "https://riskzone.anywhereportfolio.com.ar:9099/api/solicitudextraccion/"+token+"/getsolicitudextraccionbyalyc?CurrentPage=1&ItemsPerPage=100"
tokenobj = {'key': 'value'}
REQUEST_GENERAL = requests.get(URL_GENERAL, json = tokenobj)
data  = (REQUEST_GENERAL.json())
data = (data["Items"])
data = pd.DataFrame(data)
listaalycs = (data["MiembroCompensadorID"])
#ACA SE CREAN LOS DF A LOS QUE SE VA CONCATENANDO LA DATA
DATAFINAL = pd.DataFrame()
DATAFINALPORCIM = pd.DataFrame()
for alyc in listaalycs:
    listacim = []
    aloc = str(alyc)
    URL_PORALYC = "https://riskzone.anywhereportfolio.com.ar:9099/api/solicitudextraccion/"+token+"/getsolicitudextraccionbycim?AlycID="+aloc
    #tokenobj = {'key': 'value'}

    try:

        PORCIM = requests.get(URL_PORALYC)#, json = tokenobj)
        data  = (PORCIM.json())
        df = pd.DataFrame(data)
    except:
        pass
    try:
        df_cim = df["CimID"][0]
        listacim.append(df_cim)
    except:
        pass
    #SEGUNDO LOOP
    for CIMID in listacim:
         URL_PORCIM = "https://riskzone.anywhereportfolio.com.ar:9099/api/solicitudextraccion/"+token+"/getsolicitudextraccionbyneteo?AlycID="+aloc+"&CimID="+str(CIMID)
         y = requests.get(URL_PORCIM, json = tokenobj)
         SE  = (y.json())
         SE = (pd.DataFrame(SE))

         
         DATAFINALPORCIM = pd.concat([DATAFINALPORCIM, SE])

         for ID in SE["CuentaNeteoID"]:
        #esto trae solo las que son finalidad 2 (margenes), hay que agregar que tamben traiga las de FCGIM
            URL_PORFINALIDAD2 = "https://riskzone.anywhereportfolio.com.ar:9099/api/solicitudextraccion/"+token+"/getsolicitudextraccionbymensajes?AlycID="+aloc+"&CimID="+str(CIMID)+"&neteoID="+str(ID)+"&finalidadID=2"
            finalidad2 = requests.get(URL_PORFINALIDAD2, json = tokenobj)
            PORFINALIDAD  = (finalidad2.json())
            PORFINALIDAD = (pd.DataFrame(PORFINALIDAD))
            DATAFINAL = pd.concat([DATAFINAL, PORFINALIDAD])

            URL_PORFINALIDAD9 = "https://riskzone.anywhereportfolio.com.ar:9099/api/solicitudextraccion/"+token+"/getsolicitudextraccionbymensajes?AlycID="+aloc+"&CimID="+str(CIMID)+"&neteoID="+str(ID)+"&finalidadID=9"
            finalidad9 = requests.get(URL_PORFINALIDAD9, json = tokenobj)
            PORFINALIDAD  = (finalidad9.json())
            PORFINALIDAD = (pd.DataFrame(PORFINALIDAD))
            DATAFINAL = pd.concat([DATAFINAL, PORFINALIDAD])

#FUNCION PARA ENCONTRAR PROPIA #

cuentas = pd.read_csv("ListadoCIM.csv",index_col=0, encoding='latin-1')
cuentas = cuentas[cuentas["Tipo Cuenta"] == "Propia"]
cuentas =  cuentas.reset_index()
cuentas = cuentas[["MC", "MC Cód.","Cód."]]
cuentas = cuentas.rename(columns={"MC Cód.": "MC_COD", "Cód.": "MC_PROPIA_COD"})
def obtener_propia(MC_COD):
    linea = cuentas[cuentas["MC_COD"] == int(MC_COD)]
    return(int(linea["MC_PROPIA_COD"].item()))

def obtener_nombre(MC_COD):
    try:
        linea = cuentas[cuentas["MC_COD"] == int(MC_COD)]
        return(str(linea["MC"].item()))
    except:
        return(0)

    



DATAFINAL["PRIMER_ANALISIS"] = DATAFINAL["Disponible"] + DATAFINAL["Monto"]
DATAFINALPORCIM = DATAFINALPORCIM[[ "MiembroCompensadorID","CimID","CimCodigo", "CuentaNeteoID"]]
DATAFINAL = DATAFINAL.merge(DATAFINALPORCIM, on = "CuentaNeteoID", how = "left")
DATAFINAL = DATAFINAL.drop_duplicates()
MC_CIM = pd.read_excel(r"C:\Users\aggonzalez\Desktop\SE\MCCIM.xlsx")
MC = MC_CIM[["CimCodigo", "MC_Cod"]]

DATAFINAL["CimCodigo"] = DATAFINAL["CimCodigo"].astype(int)
cols = DATAFINAL.columns.tolist()
cols = cols[-1:] + cols[:-1]
DATAFINAL = DATAFINAL[cols]
DATAFINAL = DATAFINAL.merge(MC, on = "CimCodigo", how = "left")


#COLORES
def highlight_rows(row):
    value = row.loc["OUT"]
    if value == 0:
        color = '#FF0000' # Red
    elif value == 1:
        color = '#85e62c' # Green
    elif value == 2:
        color = '#ebff0a' # Otro green
    elif value == 3:
        color = "#209c05" #meio yellow
    elif value == 4:
        color = "#a8329b" #LILA BIEN FUERTE

    return ['background-color: {}'.format(color) for r in row]

#DATAFINAL.reset_index(drop=True).style.apply(highlight_rows, axis=1)

cols = DATAFINAL.columns.tolist()
cols = cols[-1:] + cols[:-1]
DATAFINAL = DATAFINAL[cols]


#SI PASA ALGO MIRAR ACA Y VOLVER A HABILITAR. TAMBIEN CAMBIAR EN LA LINEA 164 LA VARIABLE "PROPA"

#PROPIAS = pd.read_csv(r"C:\Users\aggonzalez\Desktop\SE\LISTACUENTASPROPIAS.csv")
#PROPIAS = PROPIAS.rename(columns = {'CODIGOCUENTA':'CuentaPropiaDelMC'})
#DATAFINAL = DATAFINAL.merge(PROPIAS, on = "MC_Cod", how = "left")


cuentapropia = []
for i in range(len(DATAFINAL)):
    try:
        MC_COD = int(DATAFINAL["MC_Cod"][i])
        cimpropia = obtener_propia(MC_COD)
    except:
        cimpropia = 0
    cuentapropia.append(cimpropia)

largo_maximo_nombre_alyc = 15
nombreMC = []
for i in range(len(DATAFINAL)):
    try:
        MC_COD = int(DATAFINAL["MC_Cod"][i])
        nombre_mc = obtener_nombre(MC_COD)
        nombre_mc = str(nombre_mc[:largo_maximo_nombre_alyc])
    except: 
        nombre_mc = 0
    nombreMC.append(nombre_mc)



DATAFINAL["CuentaPropiaDelMC"] = cuentapropia
DATAFINAL["ALyC"] = nombreMC

listasaldospropios = []
saldorealdelacuenta = []
neteodescrip = []
ingresoverificado = []
ingresonoverif=[]

margendeldia= []



def saldoreal(alyc, cim, neteo, finalidad):
    tokenobj = {"key":"value"}
    ENDPOINT = f"https://riskzone.anywhereportfolio.com.ar:9099/api/saldosconsolidados?MCId={alyc}&CelID={cim}&NeteoID={neteo}&FinalidadID={finalidad}"
    q = requests.get(ENDPOINT, json = tokenobj)
    SALDOREAL  = (q.json())
    SALDOREAL = pd.DataFrame(SALDOREAL)
    SALDOREAL["SALDO INICIAL POSTA"] = SALDOREAL["Cotizacion"]*SALDOREAL["SaldoInicialMoneda"]
    SALDOREALPESOS = SALDOREAL["SALDO INICIAL POSTA"].sum()
    return(SALDOREALPESOS)

# ACA SE CONSIGUEN LOS SALDOS DE LA CUENTA PROPIA 

for i in range(len(DATAFINAL)):
    ALYCID = (DATAFINAL.iloc[i,19])
    PROPA = (DATAFINAL.iloc[i,21])
    CIM = (DATAFINAL.iloc[i,20])
    NETEO = (DATAFINAL.iloc[i,2])
    FINALIDAD = (DATAFINAL.iloc[i,4])
    PROPA = cmm(PROPA)

    tokenobj = {'key': 'value'}
    URLSALDO = f"https://riskzone.anywhereportfolio.com.ar:9099/api/saldosconsolidados?MCId={ALYCID}&CelID={PROPA}"
    q = requests.get(URLSALDO, json = tokenobj)
    SALDOALYC  = (q.json())
    SALDOALYC = pd.DataFrame(SALDOALYC)
    try:
        saldoprevia = (SALDOALYC[['MiembroCompensadorID', 'CuentaCompensacionID', "MonedaDescripcion",'FinalidadID',"SaldoInicial","MargenRequeridoTotal"]]) #cambiar Anterior por Total
        saldoprevia["AP5"] = saldoprevia["SaldoInicial"] + saldoprevia["MargenRequeridoTotal"] #CAMBIAR ANTERIOR POR Total
        porfinalidad = (saldoprevia.groupby("FinalidadID").sum())
        saldo_total = (porfinalidad["AP5"].sum())
        saldodelapropia = saldo_total
    except:
        saldodelapropia= i
    listasaldospropios.append(saldodelapropia)


    


    tokenobj = {"key":"value"}
    ENDPOINT = f"https://riskzone.anywhereportfolio.com.ar:9099/api/saldosconsolidados?MCId={ALYCID}&CelID={CIM}&NeteoID={NETEO}&FinalidadID={FINALIDAD}"
    H = requests.get(ENDPOINT, json = tokenobj)
    SALDOREAL  = (H.json())
    SALDOREAL = pd.DataFrame(SALDOREAL)


# VER ACA EL TEMA DE LOS INFORMES DE PAGO EN DOLARES 
    #print(SALDOREAL.columns)

    SALDOREAL["MARGEN INICIAL POSTA"] = SALDOREAL["Cotizacion"]*SALDOREAL["MargenRequeridoAnterior"]
    SALDOREAL["SALDO INICIAL POSTA"] = SALDOREAL["Cotizacion"]*SALDOREAL["SaldoInicialMoneda"]
    SALDOREAL["SALDO INICIAL POSTA"] = SALDOREAL["SALDO INICIAL POSTA"]+SALDOREAL["MARGEN INICIAL POSTA"]
    INGRESOVERIFICADO = SALDOREAL["IngresoVerificado"].sum()
    SALDOREALPESOS = SALDOREAL["SALDO INICIAL POSTA"].sum()
    INGRESO_NO_VERIF = SALDOREAL["IngresoNoVerificado"].sum()
# este ingreso no verif deberia ser donde esta el prolbema y se necesita atencion
# ver cuando haya un IP en dolares. Se deberia ver segregado y sumado.
# LAS COLUMNAS SON ['MiembroCompensadorID', 'CuentaCompensacionID', 'FinalidadID',
#      'MonedaID', 'MonedaDescripcion', 'Cotizacion',
#      'MiembroCompensadorCodigo', 'CuentaCompensacionCodigo',
#      'FinalidadDescripcion', 'EgresoNoVerificado', 'IngresoNoVerificado',
#      'EgresoNoVerificadoMoneda', 'IngresoNoVerificadoMoneda', 'DRPRequerido',
#      'DRPRequeridoMoneda', 'SpotRequerido', 'SpotRequeridoMoneda',
#      'CuentaNeteoID', 'CuentaNeteoCodigo', 'SaldoInicial',
#      'EgresoVerificado', 'IngresoVerificado', 'SaldoInicialMoneda',
#      'EgresoVerificadoMoneda', 'IngresoVerificadoMoneda', 'CuitCuil',
#      'CuentaNeteoDescripcion', 'MargenRequeridoAnterior',
#      'MargenRequeridoDelDia', 'MargenRequeridoTotal',
#      'MargenRequeridoAnteriorSC', 'HasCuentaInfoEmpty'],


    MG2 = SALDOREAL["MargenRequeridoDelDia"].sum()



    nombrecuenta = SALDOREAL["CuentaNeteoDescripcion"].iloc[0]
    neteodescrip.append(nombrecuenta)
    ingresoverificado.append(INGRESOVERIFICADO)
    saldoposta = SALDOREALPESOS
    saldorealdelacuenta.append(saldoposta)
    margendeldia.append(MG2)
    ingresonoverif.append(INGRESO_NO_VERIF)

    #SALDO INICIAL ESTA PESIFICADO, SALDOINICIAL MONEDA ESTA EN VALOR ORIGNIAL DIEZ

DATAFINAL["Saldo de la propia"] = listasaldospropios
DATAFINAL["Saldo POSTA"] = saldorealdelacuenta
DATAFINAL["Neteo Descripcion"] = neteodescrip
DATAFINAL["Ingresos Verificados"] = ingresoverificado
DATAFINAL["Margen Requerido Del Dia"] = margendeldia
DATAFINAL["NoVerificado2"] = ingresonoverif
 
#DATAFINAL["Tipo de CIM"] = tipo_de_cuenta
if ventana == "1":
    DATAFINAL["PRIMER_ANALISIS"] = DATAFINAL["Saldo POSTA"] + DATAFINAL["Monto"] +DATAFINAL["Ingresos Verificados"]
elif ventana == "2":
    DATAFINAL["PRIMER_ANALISIS"] = DATAFINAL["Saldo POSTA"] + DATAFINAL["MargenDelDia"] + DATAFINAL["Monto"] +DATAFINAL["Ingresos Verificados"]

tipodeaprobacion = []
for i in range(len(DATAFINAL)):
    primeranalisis = int(DATAFINAL["PRIMER_ANALISIS"][i])
    SE = int(DATAFINAL["Monto"][i])
    saldoalyc =  int(DATAFINAL["Saldo de la propia"][i])
    noverificado = int(DATAFINAL["NoVerificado"][i])
    activo = DATAFINAL["ActivoDescripcion"][i]
    FCI = "FCI"

    

    if primeranalisis >= 0:
        aprobacion = 1
    else:
        if primeranalisis + saldoalyc >= 0:
            aprobacion = 3
            #print(primeranalisis + saldoalyc)
        else:
            if primeranalisis + noverificado >= 0:
                aprobacion = 2
            else:
                aprobacion = 0
    if ventana == str(2):
        if FCI in activo:
            aprobacion = 4
    elif ventana == str(1):
        pass

    tipodeaprobacion.append(aprobacion)

DATAFINAL["OUT"] = tipodeaprobacion
DATAFINAL["CimCodigo"] = DATAFINAL["CimCodigo"].astype(str)
DATAFINAL = DATAFINAL.sort_values(by = "CimCodigo")
IP = DATAFINAL[DATAFINAL["NoVerificado"] != 0]

#print(DATAFINAL.columns)
#RESHAPE DATAFRAME
DATAFINAL = DATAFINAL[["MC_Cod","ALyC","CimCodigo", "NeteoCodigo", "ActivoDescripcion","Monto", "Saldo POSTA","Ingresos Verificados", "NoVerificado", "MargenDelDia","PRIMER_ANALISIS","CuentaPropiaDelMC", "Saldo de la propia","OUT"  ]]
DATAFINAL = DATAFINAL.rename(columns={"MC_Cod":"MC","CimCodigo": "CIM", "NeteoCodigo": "Cuenta","ActivoDescripcion":"Activo","Saldo POSTA":"Saldo Inicial","NoVerificado":"Ingreso No Verificado","MargenDelDia":"Margenes","PRIMER_ANALISIS":"Saldo Consolidado Final","CuentaPropiaDelMC":"Propia MC", "Saldo de la propia":"Saldo MC"})

#FORMATO NUMEROS
DATAFINAL['Monto'] = DATAFINAL['Monto'].astype('int')
DATAFINAL.loc[:, "Monto"] = DATAFINAL["Monto"].map('{:,}'.format)

DATAFINAL['Saldo Inicial'] = DATAFINAL['Saldo Inicial'].astype('int')
DATAFINAL.loc[:, "Saldo Inicial"] = DATAFINAL["Saldo Inicial"].map('{:,}'.format)

DATAFINAL['Ingresos Verificados'] = DATAFINAL['Ingresos Verificados'].astype('int').map('{:,}'.format)
DATAFINAL['Ingreso No Verificado'] = DATAFINAL['Ingreso No Verificado'].astype('int').map('{:,}'.format)
DATAFINAL['Margenes'] = DATAFINAL['Margenes'].astype('int').map('{:,}'.format)
DATAFINAL['Saldo Consolidado Final'] = DATAFINAL['Saldo Consolidado Final'].astype('int').map('{:,}'.format)
DATAFINAL['Saldo MC'] = DATAFINAL['Saldo MC'].astype('int').map('{:,}'.format)

#DATAFINAL.to_json("JSONPRUEBA.json")

df_styled = DATAFINAL.reset_index(drop=True).style.apply(highlight_rows, axis=1)
dfi.export(df_styled, filename,max_rows=-1)
IMAGEN = Image.open(filename)
IMAGEN.show()


apro = DATAFINAL[DATAFINAL["OUT"] == 2]
apro = apro.reset_index().drop_duplicates()

if len(apro)>0:
    print("\n### SOLICITUDES CON INFORME DE PAGO A VERIFICAR ### ")
    print(apro[["MC","CIM","Ingreso No Verificado"]])
else:
    print("\n## NO HAY SOLICITUDES DE EXTRACCIÓN QUE DEPENDAN DE INFORMES DE PAGO ##")



#####################################################################
DATAFINAL["Saldo Inicial"] = DATAFINAL["Saldo Inicial"].str.replace(",","").astype(int)
DATAFINAL["Monto"] = DATAFINAL["Monto"].str.replace(",","").astype(int)
DATAFINAL["Saldo MC"] = DATAFINAL["Saldo MC"].str.replace(",","").astype(int)

DATAFINAL1 = DATAFINAL[DATAFINAL["OUT"] == 1]

#print("\n #### VERIFICAR QUE EL TOTAL DE EXTRACCIONES PARA UNA MISMA CUENTA NO SUPERE EL SALDO DE LA MISMA ### \n")

d = {'Monto':'sum', 'Saldo Inicial':'first'}
check1 = DATAFINAL1.groupby("Cuenta").agg(d)
check1["Resultado"] = check1["Saldo Inicial"] - check1["Monto"]
check1 = check1[check1["Resultado"] < 0]

if len(check1) > 0:
    print(check1)
else:
    print("\n## NINGUNA CUENTA ESTA EXTRAYENDO MÁS DEL SALDO QUE POSEE ## ")

DATAFINAL3 = DATAFINAL[DATAFINAL["OUT"] == 3]


#print("\n #### VERIFICAR QUE EL TOTAL DE EXTRACCIONES CUBIERTOS CON EL SALDO DE LA PROPIA NO SUPERE EL SALDO DE LA MISMA ### \n")

e = {'Monto':'sum', 'Saldo MC':'first'}
check2 = DATAFINAL3.groupby("Propia MC").agg(e)
check2["Resultado"] = check2["Saldo MC"] - check2["Monto"] 
check2 = check2[check2["Resultado"] < 0]

if len(check2) > 0:
    print("\n## ATENCION - EXTRACCIONES SUPERAN SALDO DE LA PROPIA: \n")
    print(check2)
else:
    print("\n## NINGUNA CUENTA PROPIA ESTA CUBRIENDO MÁS DEL SALDO QUE POSEE ##\n ")

