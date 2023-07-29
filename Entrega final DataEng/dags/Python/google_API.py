
from datetime import timedelta,datetime, date
import pandas as pd
from dateutil.relativedelta import relativedelta
from pytrends.request import TrendReq
from sqlalchemy import create_engine
import psycopg2
from psycopg2 import extras
import os

def conectar_API_Region():
    try:
            
            fecha_inic=date.today().strftime("%Y-%m-%d")
            fecha_fin=date.today()-relativedelta(months=1)
            fecha_fin=fecha_fin.strftime("%Y-%m-%d")
            print(f"Obteniendo datos por región, fecha de referencia: {fecha_inic}")
            pt = TrendReq(hl="es-AR", tz=902, timeout=(10,25))
            kw_list= ["Larreta", "Guillermo Moreno", "Milei", "Patricia Bullrich", "Scioli", "Juan Grabois", "Pichetto", "Facundo Manes", "Wado de Pedro", "Sergio Massa"]
            aux=0

            for element in kw_list:
                pt.build_payload([element], timeframe="{} {}".format(fecha_fin, fecha_inic), geo="AR", cat=396)
                
                if aux==0:
                    ibr=pt.interest_by_region("city", inc_low_vol=True, inc_geo_code=True)
                    aux=1
                else:
                    ibr_aux = pt.interest_by_region("city", inc_low_vol=True, inc_geo_code=True)
                    #elimino columna no necesaria
                    ibr_aux.drop(columns='geoCode', inplace=True)
                    ibr=pd.merge(ibr, ibr_aux, left_index=True, right_index=True)
            
            ibr['Mes']=date.today().strftime("%b")
            ibr.reset_index(inplace=True)
            
            return ibr

    except Exception as e:
            print("Ocurrio el siguiente error; ", e)
            raise e


def cargar_data_region(ibr):
        try:
            print(f"Cargando la data por región")
            connect=psycopg2.connect(host=os.getenv("AWS_REDSHIFT_HOST"), dbname=os.getenv("AWS_REDSHIFT_DBNAME"), user=os.getenv("AWS_REDSHIFT_USER"), password=os.getenv("AWS_REDSHIFT_PASSWORD"), port=os.getenv("AWS_REDSHIFT_PORT"))
            cur=connect.cursor()
            schema=os.getenv("AWS_REDSHIFT_SCHEMA")
            connect.autocommit = True

            cur.execute(f"""
            create table if not exists {schema}.Consultas_por_region(
                geoname VARCHAR(50),
                geoCode VARCHAR(4) distkey,
                Larreta smallint,
                Guillermo_Moreno smallint,
                Milei smallint,
                Patricia_Bullrich smallint,
                Scioli smallint,
                Juan_Grabois smallint,
                Pichetto smallint,
                Facundo_Manes smallint,
                Wado_de_Pedro smallint,
                Sergio_Massa smallint,
                mes VARCHAR(20)
                
            ) sortkey(mes);
            """)
            cur.execute(f"""
            create table if not exists {schema}.Consultas_por_region_staging(
                geoname VARCHAR(50),
                geoCode VARCHAR(4) distkey,
                Larreta smallint,
                Guillermo_Moreno smallint,
                Milei smallint,
                Patricia_Bullrich smallint,
                Scioli smallint,
                Juan_Grabois smallint,
                Pichetto smallint,
                Facundo_Manes smallint,
                Wado_de_Pedro smallint,
                Sergio_Massa smallint,
                mes VARCHAR(20)
                
            ) sortkey(mes);
            """) 

            extras.execute_values(
                        cur=cur,
                        sql="""
                            INSERT INTO Consultas_por_region_staging
                            (geoname, geoCode, Larreta, Guillermo_Moreno, Milei, Patricia_Bullrich, Scioli,Juan_Grabois, Pichetto,
                            Facundo_Manes, Wado_de_Pedro, Sergio_Massa, mes)
                            VALUES %s;
                            """,
                        argslist=ibr.to_dict(orient="records"),
                        template="""
                            (
                                %(geoName)s, %(geoCode)s, %(Larreta)s,
                                %(Guillermo Moreno)s, %(Milei)s, %(Patricia Bullrich)s,
                                %(Scioli)s, %(Juan Grabois)s, %(Pichetto)s, %(Facundo Manes)s,
                                %(Wado de Pedro)s, %(Sergio Massa)s, %(Mes)s
                            )
                            """
                    )
            connect.commit()
            cur.execute(f"""
            begin transaction;

            delete from consultas_por_region using consultas_por_region_staging
            where consultas_por_region.geocode=consultas_por_region_staging.geocode
            and consultas_por_region.mes=consultas_por_region_staging.mes;

            insert into consultas_por_region select * from consultas_por_region_staging;

            delete from consultas_por_region_staging;

            end transaction;
            """)

            connect.commit()
            cur.close()
            connect.close()
        except Exception as e:
            print("Ocurrio el siguiente error; ", e)


def conectar_API_Fecha():
    try:
        fecha_inic=date.today().strftime("%Y-%m-%d")
        print(f"Obteniendo datos por fecha, fecha de referencia: {fecha_inic}")
        pt = TrendReq(hl="es-AR", tz=902, timeout=(10,25))
            
        kw_list= ["Larreta", "Guillermo Moreno", "Milei", "Patricia Bullrich", "Scioli", "Juan Grabois", "Pichetto", "Facundo Manes", "Wado de Pedro", "Sergio Massa"]

        aux=0

        for element in kw_list:
            pt.build_payload([element], timeframe="today 3-m", geo="AR", cat=396)
            
            if aux==0:
                iot = pt.interest_over_time()
                #elimino columna no necesaria
                iot.drop(columns='isPartial', inplace=True)
                aux=1
            else:
                iot_aux = pt.interest_over_time()
                iot_aux.drop(columns='isPartial', inplace=True)
                iot=pd.merge(iot, iot_aux, left_index=True, right_index=True)
        
        iot.reset_index(inplace=True)
                    
        return iot

    except Exception as e:
            print("Ocurrio el siguiente error; ", e)
            raise e


def cargar_data_fecha(iot):
        try:
            print(f"Cargando la data por fecha")
            connect=psycopg2.connect(host=os.getenv("AWS_REDSHIFT_HOST"), dbname=os.getenv("AWS_REDSHIFT_DBNAME"), user=os.getenv("AWS_REDSHIFT_USER"), password=os.getenv("AWS_REDSHIFT_PASSWORD"), port=os.getenv("AWS_REDSHIFT_PORT"))
            cur=connect.cursor()
            schema=os.getenv("AWS_REDSHIFT_SCHEMA")
            connect.autocommit = True

            cur.execute(f"""
            create table if not exists {schema}.Consultas_por_fecha(
                date VARCHAR(10) distkey,
                Larreta smallint,
                Guillermo_Moreno smallint,
                Milei smallint,
                Patricia_Bullrich smallint,
                Scioli smallint,
                Juan_Grabois smallint,
                Pichetto smallint,
                Facundo_Manes smallint,
                Wado_de_Pedro smallint,
                Sergio_Massa smallint
                
            ) sortkey(date);
            """)
            cur.execute(f"""
            create table if not exists {schema}.Consultas_por_fecha_staging(
                date VARCHAR(10) distkey,
                Larreta smallint,
                Guillermo_Moreno smallint,
                Milei smallint,
                Patricia_Bullrich smallint,
                Scioli smallint,
                Juan_Grabois smallint,
                Pichetto smallint,
                Facundo_Manes smallint,
                Wado_de_Pedro smallint,
                Sergio_Massa smallint
                
            ) sortkey(date);
            """)

            extras.execute_values(
                    cur=cur,
                    sql="""
                        INSERT INTO Consultas_por_fecha_staging
                        (date, Larreta, Guillermo_Moreno, Milei, Patricia_Bullrich, Scioli,Juan_Grabois, Pichetto,
                        Facundo_Manes, Wado_de_Pedro, Sergio_Massa)
                        VALUES %s;
                        """,
                    argslist=iot.to_dict(orient="records"),
                    template="""
                        (
                            %(date)s, %(Larreta)s,
                            %(Guillermo Moreno)s, %(Milei)s, %(Patricia Bullrich)s,
                            %(Scioli)s, %(Juan Grabois)s, %(Pichetto)s, %(Facundo Manes)s,
                            %(Wado de Pedro)s, %(Sergio Massa)s
                        )
                        """
                )
            

            connect.commit()
            cur.execute(f"""
            begin transaction;

            delete from consultas_por_fecha using consultas_por_fecha_staging
            where consultas_por_fecha.date=consultas_por_fecha_staging.date;

            insert into consultas_por_fecha select * from consultas_por_fecha_staging;

            delete from consultas_por_fecha_staging;

            end transaction;
            """)
            connect.commit()
            cur.close()
            connect.close()
        except Exception as e:
            print("Ocurrio el siguiente error; ", e)


