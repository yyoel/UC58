import json
from json.decoder import JSONDecodeError
from typing import Hashable
import re

import pika
import sqlalchemy
from sqlalchemy.orm import sessionmaker

from model.models import Base, HC_EmployeeCareer


def main():
    # Create connection to MSSQL DWHDB
    engine = sqlalchemy.create_engine(
        "mssql://biread:DWBfi35tahun@dwhdb/UC_DEV?driver=SQL Server")
    Session = sessionmaker(bind=engine)
    session = Session()

    # RabbitMQ Connection 
    credentials = pika.PlainCredentials("guest", "guest")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="172.16.4.14", credentials=credentials)
    )

    channel = connection.channel()

    def callback(ch, method, properties, body):

        # Turn JSON into Python Dictionary
        print('Before Decode', body)
        data = json.loads(body, strict=False)
        print('After Decode', data)

        # Save Data
        save_to_db(data)

    def save_to_db(data):
        exist = session.query(HC_EmployeeCareer.emp_no).filter_by(emp_no=data["emp_no"]).scalar()
        try:
            if exist is None:
                employee = HC_EmployeeCareer(
                    data["emp_no"],
                    data["emp_name"],
                    data["emp_status"],
                    data["dt_join"],
                    data["dt_rehire_mitra"],
                    data["dt_resign"],
                    data["dt_birthday"],
                    data["company_id"],
                    data["company_name"],
                    data["directorate_code"],
                    data["directorate_name"],
                    data["division_code"],
                    data["division_name"],
                    data["department_code"],
                    data["department_name"],
                    data["subdepartment_code"],
                    data["subdepartment_name"],
                    data["section_code"],
                    data["section_name"],
                    data["job_title_code"],
                    data["job_title_name"],
                    data["position_code"],
                    data["position_name"],
                    data["job_grade"],
                    data["worklocation_code"],
                    data["worklocation_name"],
                    data["psikotest"],
                    data["corporate_email"],
                    data["mobile_phone_no"],
                    data["superior_emp_no"],
                    data["superior_emp_name"],
                    data["gender"],
                    data["education"],
                    data["education_institution_name_major"],
                    data["work_experience_1_company"],
                    data["work_experience_1_position"],
                    data["work_experience_1_start"],
                    data["work_experience_1_end"],
                    data["resign_reason"],
                    data["last_career_type"],
                    data["directorate_catalog_code"],
                    data["directorate_catalog_name"],
                    data["division_catalog_code"],
                    data["division_catalog_name"],
                    data["department_catalog_code"],
                    data["department_catalog_name"],
                    data["unit_catalog_code"],
                    data["unit_catalog_name"],
                    data["satuan_kerja_code"],
                    data["satuan_kerja_name"],
                    data["measure_id"],
                    data["measure_name"],
                    data["cost_allocation_code"],
                    data["cost_allocation_name"],
                    data["region_allocation_code"],
                    data["region_allocation_name"],
                    data["religion"],
                    data["marital_status"],
                    data["num_of_dependence"],
                    data["PlaceOfBirth"],
                    data["BranchRecruitment"],
                )

                session.add(employee)
                session.commit()
                # print('Object',employee)
            else:
                print("This data", data["emp_no"], "already exist")
        except JSONDecodeError:
            print('CONTAIN ERROR')

    # Create Table 
    if HC_EmployeeCareer.__tablename__ not in engine.table_names():
        Base.metadata.tables["UC58_HC_EmployeeCareer"].create(bind=engine)
        print("Table created.")

    # Consume Message
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="q.human-capital.master-employee-data-analytics.work",
                          auto_ack=True,
                          on_message_callback=callback)

    print("[x] Waiting for Message. To exit press CTRL+C")
    channel.start_consuming()
    connection.close()


if __name__ == "__main__":
    main()
