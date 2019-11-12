#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import namedtuple
from datetime import datetime
import logging
import json
import requests
import warnings

import numpy as np
import pandas as pd
import pymysql
import urllib.request

from pyfcm import FCMNotification
from scipy.spatial import cKDTree
from sqlalchemy import create_engine

warnings.simplefilter(action='ignore', category=FutureWarning)
push_service = FCMNotification(api_key="AAAAVIVJ3xo:APA91bFvnWh-uDWoRPb8PUDITkVvNQUHxTR8T0z964R-3GdIZJa0BwoEyEt3Xv2RNPXydBobC5PwJRT8blozBhUb7vKzbvmfMxrCRdR2S6X48QizXetG-xV4JRQ1xLOlpAuUyrvTVwim")
ora_adesso = datetime.now().hour

def controlla_anomalie(df, media, dev_std, numero_deviazioni):
    """Calcolo i V-values"""
    # Accetta una sola colonna del df
    V_values = (df - media) / dev_std
    return V_values > numero_deviazionio_numeric(df.lng)

def lingue(choosen_one_s, livello_pericolo):
    """Seleziona le lingue, se non ci sono è inglese."""
    lingue = ["IT", "DE", "FR", "ES"]
    utenti_lingua = [choosen_one_s.hl == lingua for lingua in lingue]
    # Quelli che non sono nelle lingue sopra
    utenti_lingua.append(pd.concat(utenti_lingua, axis=1).sum(axis=1) == 0)
    Traduzioni = namedtuple('Traduzioni', 'message_title message_body')
    if livello_pericolo == "df_2":
        italiano = Traduzioni("Inquinamento insolito", "Rilevati livelli insolitamente alti di inquinamento nell'area, prestare particolare attenzione.")
        tedesco = Traduzioni("Ungewöhnlicher Verschmutzungsgrad", "Beachten Sie bitte, dass ungewöhnlich viele Schadstoffe festgestellt werden.")
        francese = Traduzioni("Niveau de pollution inhabituel", "Niveau inhabituellement élevé de polluants détectés, veuillez faire attention.")
        spagnolo = Traduzioni("Nivel de contaminación inusual", "Nivel inusualmente alto de contaminantes detectados, tenga cuidado".)
        altre_lingue = Traduzioni("Unusual pollution level", "Unusually high level of pollutants detected, please be careful.")
    else:
        italiano = Traduzioni("Inquinamento pericoloso", "Rilevati livelli estremamenti alti di inquinamento nell'area, prestare particolare attenzione.")
        tedesco = Traduzioni("Gefährliche Verschmutzung", "Sehr hohe Schadstoffkonzentration, bitte besondere Aufmerksamkeit zu widmen.")
        francese = Traduzioni("Niveau de pollution dangereux", "Très haute concentration de polluants détectés, soyez particulièrement prudent.")
        spagnolo = Traduzioni("Nivel de contaminación peligrosa", "Nivel extremadamente alto de contaminantes detectados, tenga especial cuidado".)
        altre_lingue = Traduzioni("Dangerous pollution level", "Extremely high level of pollutants detected, please be especially careful.")
    return utenti_lingua, [italiano, tedesco, francese, altre_lingue], lingue.append("EN")

def seleziona_e_invia(store, sensori_selezionati, df_users, nome_database, location_sensori):
    """Se ci sono anomalie fuori dalle 3 std
    Le coordinate dei sensori fuori dalle 3 std:"""
    # Tempo tra le notifiche
    tempo_tra_notifiche = 6
    # Obbligatorio qui per evitare i Nan
    coo_utenti = [(lat,lon) for lat,lon in zip(df_users.lat, df_users.lng)]
    # Migliore con molti dati: coo_sensori = list(sensori_selezionati.itertuples(index=False, name=None))
    coo_sensori = [(lat,lon) for lat,lon in zip(sensori_selezionati.lat, sensori_selezionati.lng)]
    idx = verifica_distanza(loc_users=coo_utenti, points_sensori=coo_sensori)
    # Selezione utenti vicini
    choosen_one_s = df_users.loc[idx]
    # Segno l'ora a cui li ho avvisati come 50, così se aspetto n ore < 56 per avvisare comunque ho {x in 24} - -50 > n
    tempo_sicurezza = -50
    choosen_one_s["ora_avviso"] = tempo_sicurezza
    # Controlla chi ho già avvisato
    df_store = store[nome_database]
    try:
        # Prendo quelli scelti questa volta e ci metto l'ora a cui erano stati scelti l'altra volta
        # Concateno df_store e i prescelti, aggiungo un indice, cerco i duplicati e copio la vecchia ora di avviso nella nuova
        gia_notificati = pd.concat([df_store, choosen_one_s], keys=['s1', 's2']).tkn.duplicated(keep=False)
        choosen_one_s.loc[gia_notificati["s2"], "ora_avviso"] = df_store[gia_notificati["s1"]].ora_avviso
    except AttributeError:
        # Se df_store è vuoto non ha la colonna tkn
        None
    except ValueError:
        # Se df_store è vuoto non posso compararlo
        None
    except KeyError as e:
        # Se l'errore è in s2 choosen_one_s è vuoto e non c'è nessuno da avvisare
        if e.args[0] == "s2":
            # Restituisce tutti quelli avvisati da poco ma non questa volta
            return df_store[(ora_adesso - df_store.ora_avviso) < tempo_tra_notifiche]
        None
    # Seleziono quelli che non avviso da 6 ore
    # 6 le ore che aspetto prima di avvisare di nuovo
    overtime = choosen_one_s[(ora_adesso - choosen_one_s.ora_avviso) > tempo_tra_notifiche]
    if not choosen_one_s.equals(df_store) or overtime.size > 0:
        # Salva gli utenti che sto per avvisare e correggi gli orari
        choosen_one_s.loc[choosen_one_s.ora_avviso == tempo_sicurezza, "ora_avviso"] = ora_adesso
        store.put(nome_database, choosen_one_s)
        # Seleziona i giusti gruppi di utenti, se non c'è la lingua manda in inglese
        utenti, traduzioni, lingue = lingue(choosen_one_s, nome_database)
        map(lambda traduzione, utenti_lingua, lingua: send_mess(registration_ids=overtime.loc[utenti_lingua].tkn.values.tolist(), message_title=traduzione.message_title, message_body=traduzione.message_body, location_sensori=location_sensori, lingua=lingua), zip(traduzioni, utenti, lingue))
    # Tutti quelli da non avvisare
    return df_store[(ora_adesso - df_store.ora_avviso) < tempo_tra_notifiche]
