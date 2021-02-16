import json
from json.decoder import JSONDecodeError
from typing import Hashable
import re
import datetime

import pika
import sqlalchemy
from sqlalchemy.orm import sessionmaker

from model.models import Base, UC58HCEmployeeCareer

MAX_RETRY = 3
QUEUE_INSERT = 'q.human-capital.master-employee-data-analytics.work'
# QUEUE_UPDATE = 'q.confins.los-update-data-cust.work'
ROUTING_KEY = 'human-capital.master-employee-da'
EXCHANGE = 'x.human-capital.dead'


# Create connection to MSSQL DWHDB
engine = sqlalchemy.create_engine(
    "mssql+pyodbc://biread:DWBfi35tahun@dwhdb/UC_DEV?driver=ODBC Driver 17 for SQL Server")
Session = sessionmaker(bind=engine)
session = Session()

# RabbitMQ Connection 
credentials = pika.PlainCredentials("guest", "guest")
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="172.16.5.91", port=4010, credentials=credentials)
)

channel = connection.channel()
print('[*] Waiting for messages. To exit press CTRL+C')

def callback(ch, method, properties, body):

    def create_tables():
        Base.metadata.create_all(bind=engine)

    # Turn JSON into Python Dictionary
    data = None
    try:
        data = json.loads(body, strict=False)
        print('[+] Received! \n', body)

        # Create table
        if UC58HCEmployeeCareer.__tablename__ not in engine.table_names():
            create_tables()
        # Save or Update Data
        if data:
            save_to_db(data=data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except JSONDecodeError:
        headers = properties.headers
        count_retry = int(headers["x-death"][0]["count"]) if headers.get("x-death") else 0
        x_death_reason = headers["x-first-death-reason"] if headers.get("x-first-death-reason") else None
        x_death_queue = headers["x-first-death-queue"] if headers.get("x-first-death-queue") else None

        if count_retry <= MAX_RETRY:
            print(f'[!] Reason ({x_death_reason}) Unhandled data, Queue ({x_death_queue}), Retry Count ({count_retry})!\n', body)
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        else:
            print(f'[!] Reason ({x_death_reason}) Unhandled data, Queue ({x_death_queue}), Retry Count ({count_retry})!\n', body)
            ch.basic_publish(exchange=EXCHANGE,
                             routing_key=ROUTING_KEY,
                             body=body,
                             properties=pika.BasicProperties(delivery_mode=1))
            ch.basic_ack(delivery_tag=method.delivery_tag)

def save_to_db(data):
    exist = session.query(UC58HCEmployeeCareer).filter(UC58HCEmployeeCareer.emp_no==data["emp_no"]).all()
    try:
        if not exist :
            employee = UC58HCEmployeeCareer(
                emp_no=data["emp_no"],
                emp_name=data["emp_name"],
                emp_status=data["emp_status"],
                dt_join=data["dt_join"] if data["dt_join"] else None,
                dt_rehire_mitra=data["dt_rehire_mitra"] if data["dt_rehire_mitra"] else None,
                dt_resign=data["dt_resign"] if data ["dt_resign"] else None,
                dt_birthday=data["dt_birthday"] if data["dt_birthday"] else None,
                company_id=data["company_id"],
                company_name=data["company_name"],
                directorate_code=data["directorate_code"],
                directorate_name=data["directorate_name"],
                division_code=data["division_code"],
                division_name=data["division_name"],
                department_code=data["department_code"],
                department_name=data["department_name"],
                subdepartment_code=data["subdepartment_code"],
                subdepartment_name=data["subdepartment_name"],
                section_code=data["section_code"],
                section_name=data["section_name"],
                job_title_code=data["job_title_code"],
                job_title_name=data["job_title_name"],
                position_code=data["position_code"],
                position_name=data["position_name"],
                job_grade=data["job_grade"],
                worklocation_code=data["worklocation_code"],
                worklocation_name=data["worklocation_name"],
                homebase_code=data["homebase_code"],
                homebase_name=data["homebase_name"],
                dt_last_career_eff=data["dt_last_career_eff"] if data["dt_last_career_eff"] else None,
                religion=data["religion"],
                marital_status=data["marital_status"],
                num_of_dependance=data["num_of_dependance"],
                place_of_birth=data["place_of_birth"],
                psikotest=data["psikotest"],
                corporate_email=data["corporate_email"],
                mobile_phone_no=data["mobile_phone_no"],
                superior_emp_no=data["superior_emp_no"],
                superior_emp_name=data["superior_emp_name"],
                gender=data["gender"],
                education=data["education"],
                education_institution_name_major=data["education_institution_name_major"],
                work_experience_1_company=data["work_experience_1_company"],
                work_experience_1_position=data["work_experience_1_position"],
                work_experience_1_start=data["work_experience_1_start"],
                work_experience_1_end=data["work_experience_1_end"],
                resign_reason=data["resign_reason"],
                last_career_type=data["last_career_type"],
                directorate_catalog_code=data["directorate_catalog_code"],
                directorate_catalog_name=data["directorate_catalog_name"],
                division_catalog_code=data["division_catalog_code"],
                division_catalog_name=data["division_catalog_name"],
                department_catalog_code=data["department_catalog_code"],
                department_catalog_name=data["department_catalog_name"],
                unit_catalog_code=data["unit_catalog_code"],
                unit_catalog_name=data["unit_catalog_name"],
                satuan_kerja_code=data["satuan_kerja_code"],
                satuan_kerja_name=data["satuan_kerja_name"],
                measure_id=data["measure_id"],
                measure_name=data["measure_name"],
                cost_allocation_code=data["cost_allocation_code"],
                cost_allocation_name=data["cost_allocation_name"],
                region_allocation_code=data["region_allocation_code"],
                region_allocation_name=data["region_allocation_name"]
            )

            session.add(employee)
            session.commit()          
            print("[+] Data Inserted!")
        else:
            employee: UC58HCEmployeeCareer
            for employee in exist:
                employee.emp_no=data["emp_no"],
                employee.emp_name=data["emp_name"],
                employee.emp_status=data["emp_status"],
                employee.emp_prev_no=data["emp_prev_no"],
                employee.dt_join=data["dt_join"] if data["dt_join"] else None,
                employee.dt_rehire_mitra=data["dt_rehire_mitra"] if data["dt_rehire_mitra"] else None,
                employee.dt_resign=data["dt_resign"] if data["dt_resign"] else None,
                employee.dt_birthday=data["dt_birthday"] if data["dt_birthday"] else None,
                employee.company_id=data["company_id"],
                employee.company_name=data["company_name"],
                employee.directorate_code=data["directorate_code"],
                employee.directorate_name=data["directorate_name"],
                employee.division_code=data["division_code"],
                employee.division_name=data["division_name"],
                employee.department_code=data["department_code"],
                employee.department_name=data["department_name"],
                employee.subdepartment_code=data["subdepartment_code"],
                employee.subdepartment_name=data["subdepartment_name"],
                employee.section_code=data["section_code"],
                employee.section_name=data["section_name"],
                employee.job_title_code=data["job_title_code"],
                employee.job_title_name=data["job_title_name"],
                employee.position_code=data["position_code"],
                employee.position_name=data["position_name"],
                employee.job_grade=data["job_grade"],
                employee.worklocation_code=data["worklocation_code"],
                employee.worklocation_name=data["worklocation_name"],
                employee.homebase_code=data["homebase_code"],
                employee.homebase_name=data["homebase_name"],
                employee.dt_last_career_eff=data["dt_last_career_eff"] if data["dt_last_career_eff"] else None,
                employee.religion=data["religion"],
                employee.marital_status=data["marital_status"],
                employee.num_of_dependance=data["num_of_dependance"],
                employee.place_of_birth=data["place_of_birth"],
                employee.psikotest=data["psikotest"],
                employee.corporate_email=data["corporate_email"],
                employee.mobile_phone_no=data["mobile_phone_no"],
                employee.superior_emp_no=data["superior_emp_no"],
                employee.superior_emp_name=data["superior_emp_name"],
                employee.gender=data["gender"],
                employee.education=data["education"],
                employee.education_institution_name_major=data["education_institution_name_major"],
                employee.work_experience_1_company=data["work_experience_1_company"],
                employee.work_experience_1_position=data["work_experience_1_position"],
                employee.work_experience_1_start=data["work_experience_1_start"],
                employee.work_experience_1_end=data["work_experience_1_end"],
                employee.resign_reason=data["resign_reason"],
                employee.last_career_type=data["last_career_type"],
                employee.directorate_catalog_code=data["directorate_catalog_code"],
                employee.directorate_catalog_name=data["directorate_catalog_name"],
                employee.division_catalog_code=data["division_catalog_code"],
                employee.division_catalog_name=data["division_catalog_name"],
                employee.department_catalog_code=data["department_catalog_code"],
                employee.department_catalog_name=data["department_catalog_name"],
                employee.unit_catalog_code=data["unit_catalog_code"],
                employee.unit_catalog_name=data["unit_catalog_name"],
                employee.satuan_kerja_code=data["satuan_kerja_code"],
                employee.satuan_kerja_name=data["satuan_kerja_name"],
                employee.measure_id=data["measure_id"],
                employee.measure_name=data["measure_name"],
                employee.cost_allocation_code=data["cost_allocation_code"],
                employee.cost_allocation_name=data["cost_allocation_name"],
                employee.region_allocation_code=data["region_allocation_code"],
                employee.region_allocation_name=data["region_allocation_name"]

            session.commit()
            print("[+] Data Updated!")
    except JSONDecodeError:
        print('CONTAIN ERROR')

# Consume Message
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=QUEUE_INSERT, on_message_callback=callback)

try:
	channel.start_consuming()
except KeyboardInterrupt:
	channel.stop_consuming();
