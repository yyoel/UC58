from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class HC_EmployeeCareer(Base):
    __tablename__ = 'UC58_HC_EmployeeCareer'

    id = Column(Integer, primary_key=True)
    emp_no = Column(String)
    emp_name = Column(String)
    emp_status = Column(String)
    dt_join = Column(Date)
    dt_rehire_mitra = Column(Date)
    dt_resign = Column(Date)
    dt_birthday = Column(Date)
    company_id = Column(String)
    company_name = Column(String)
    directorate_code = Column(String)
    directorate_name = Column(String)
    division_code = Column(String)
    division_name = Column(String)
    department_code = Column(String)
    department_name = Column(String)
    subdepartment_code = Column(String)
    subdepartment_name = Column(String)
    section_code = Column(String)
    section_name = Column(String)
    job_title_code = Column(String)
    job_title_name = Column(String)
    position_code = Column(String)
    position_name = Column(String)
    job_grade = Column(String)
    worklocation_code = Column(String)
    worklocation_name = Column(String)
    psikotest = Column(String)
    corporate_email = Column(String)
    mobile_phone_no = Column(String)
    superior_emp_no = Column(String)
    superior_emp_name = Column(String)
    gender = Column(String)
    education = Column(String)
    education_institution_name_major = Column(String)
    work_experience_1_company = Column(String)
    work_experience_1_position = Column(String)
    work_experience_1_start = Column(String)
    work_experience_1_end = Column(String)
    resign_reason = Column(String)
    last_career_type = Column(String)
    directorate_catalog_code = Column(String)
    directorate_catalog_name = Column(String)
    division_catalog_code = Column(String)
    division_catalog_name = Column(String)
    department_catalog_code = Column(String)
    department_catalog_name = Column(String)
    unit_catalog_code = Column(String)
    unit_catalog_name = Column(String)
    satuan_kerja_code = Column(String)
    satuan_kerja_name = Column(String)
    measure_id = Column(String)
    measure_name = Column(String)
    cost_allocation_code = Column(String)
    cost_allocation_name = Column(String)
    region_allocation_code = Column(String)
    religion = Column(String)
    marital_status = Column(String)
    num_of_dependence = Column(Integer)
    PlaceOfBirth = Column(String)
    BranchRecruitment = Column(String)

    def __init__(self, emp_no, emp_name, emp_status, dt_join, dt_rehire_mitra,
                 dt_resign, dt_birthday, company_id, company_name, directorate_code, directorate_name,
                 division_code, division_name, department_code, department_name, subdepartment_code, subdepartment_name,
                 section_code, section_name, job_title_code, job_title_name, position_code, position_name, job_grade, worklocation_code, worklocation_name,
                 psikotest, corporate_email, mobile_phone_no, superior_emp_no, superior_emp_name, gender, education, education_institution_name_major, 
                 work_experience_1_company, work_experience_1_position, work_experience_1_start, work_experience_1_end, resign_reason, last_career_type,
                 directorate_catalog_code, directorate_catalog_name, division_catalog_code, division_catalog_name, department_catalog_code, department_catalog_name,
                 unit_catalog_code, unit_catalog_name, satuan_kerja_code, satuan_kerja_name, measure_id, measure_name, cost_allocation_code, cost_allocation_name,
                 region_allocation_code, region_allocation_name, religion, marital_status, num_of_dependence, PlaceOfBirth, BranchRecruitment):

                 self.emp_no = emp_no
                 self.emp_name = emp_name
                 self.emp_status = emp_status
                 self.dt_join = dt_join
                 self.dt_rehire_mitra = dt_rehire_mitra
                 self.dt_resign = dt_resign
                 self.dt_birthday = dt_birthday
                 self.company_id = company_id
                 self.company_name = company_name
                 self.directorate_code = directorate_code
                 self.directorate_name = directorate_name
                 self.division_code = division_code
                 self.division_name = division_name
                 self.department_code = department_code
                 self.department_name = department_name
                 self.subdepartment_code = subdepartment_code
                 self.subdepartment_name = subdepartment_name
                 self.section_code = section_code
                 self.section_name = section_name
                 self.job_title_code = job_title_code
                 self.job_title_name = job_title_name
                 self.position_code = position_code
                 self.position_name = position_name
                 self.job_grade = job_grade
                 self.worklocation_code = worklocation_code
                 self.worklocation_name = worklocation_name
                 self.psikotest = psikotest
                 self.corporate_email = corporate_email
                 self.mobile_phone_no = mobile_phone_no
                 self.superior_emp_no = superior_emp_no
                 self.superior_emp_name = superior_emp_name
                 self.gender = gender
                 self.education = education
                 self.education_institution_name_major = education_institution_name_major
                 self.work_experience_1_company = work_experience_1_company
                 self.work_experience_1_position = work_experience_1_position
                 self.work_experience_1_start = work_experience_1_start
                 self.work_experience_1_end = work_experience_1_end
                 self.resign_reason = resign_reason
                 self.last_career_type = last_career_type
                 self.directorate_catalog_code =directorate_catalog_code
                 self.directorate_catalog_name = directorate_catalog_name
                 self.division_catalog_code = division_catalog_code
                 self.division_catalog_name = division_catalog_name
                 self.department_catalog_code = department_catalog_code
                 self.department_catalog_name = department_catalog_name
                 self.unit_catalog_code = unit_catalog_code
                 self.unit_catalog_name = unit_catalog_name
                 self.satuan_kerja_code = satuan_kerja_code
                 self.satuan_kerja_name = satuan_kerja_name
                 self.measure_id = measure_id
                 self.measure_name = measure_name
                 self.cost_allocation_code = cost_allocation_code
                 self.cost_allocation_name = cost_allocation_name
                 self.region_allocation_code = region_allocation_code
                 self.region_allocation_name = region_allocation_name
                 self.religion = religion
                 self.marital_status = marital_status
                 self.num_of_dependence = num_of_dependence
                 self.PlaceOfBirth = PlaceOfBirth
                 self.BranchRecruitment = BranchRecruitment
