from calendar import c
import pandas as pd
import requests

cuentas = pd.read_excel(r"C:\Users\aggonzalez\Desktop\SE\obtener_cim_id.xlsx")
archivocuentas = pd.read_excel(r"C:\Users\aggonzalez\Desktop\SE\TipoDeCuentaPorCim.xlsx")

def cim_to_id(cim):
    try:
        id=cuentas[cuentas["CuentaCompensacionCodigo"]== int(cim)].iloc[0,0]
        return(id)
    except:
        return(cim)

def tipodecuenta(cod):
    tipo = archivocuentas[archivocuentas["CimCodigo"] == (cod)].iloc[0,1]
    return(tipo)

