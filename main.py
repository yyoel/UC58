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
                    emp_no=data["emp_no"],
                    emp_name=data["emp_name"],
                    emp_status=data["emp_status"],
                    dt_join=data["dt_join"],
                    dt_rehire_mitra=data["dt_rehire_mitra"],
                    dt_resign=data["dt_resign"],
                    dt_birthday=data["dt_birthday"],
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
                    region_allocation_name=data["region_allocation_name"],
                    religion=data["religion"],
                    marital_status=data["marital_status"],
                    num_of_dependence=data["num_of_dependence"],
                    PlaceOfBirth=data["place_of_birth"],
                    BranchRecruitment=data["homebase_name"],
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
        Base.metadata.tables["UC58_HC_EmployeeCareer_2"].create(bind=engine)
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
