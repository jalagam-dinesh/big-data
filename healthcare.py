# Importing data preprocessing lib
import pandas as pd

# Import mysql connector and stablish connection
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="root"
)

print(mydb) 

# Print the databases (list of databses)
mycursor = mydb.cursor()

mycursor.execute("show databases")
myresult = mycursor.fetchall()

for x in myresult:
    print(x)

# Create new database

mycursor.execute("CREATE DATABASE healthcare")

# Use new database

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="root",
    database="healthcare"
)

mycursor = mydb.cursor()

# Create new table groups

mycursor.execute("""CREATE TABLE groups_tble (grp_sk int NOT NULL UNIQUE AUTO_INCREMENT,grp_id VARCHAR(6) NOT NULL PRIMARY KEY,
                 grp_name VARCHAR(90),premium_written int NOT NULL,city VARCHAR(20),
                 zip_code int,country VARCHAR(5),grp_type VARCHAR(10))""")

# Create new table subgroup
mycursor.execute("""CREATE TABLE subgroup (subgrp_sk int NOT NULL UNIQUE AUTO_INCREMENT,subgrp_id VARCHAR(4) NOT NULL PRIMARY KEY,
                 subgrp_name VARCHAR(90),monthly_premium float(6,2))""")

# Create new table group_subgroup
mycursor.execute("""CREATE TABLE group_subgroup (grpsub_sk int NOT NULL UNIQUE AUTO_INCREMENT,
                    g_id VARCHAR(6) NOT NULL,
                    s_id VARCHAR(4) NOT NULL,
                    FOREIGN KEY(g_id) REFERENCES groups_tble(grp_id),
                    FOREIGN KEY(s_id) REFERENCES subgroup(subgrp_id))""")
# Create new table disease
mycursor.execute("""CREATE TABLE disease (disease_id int NOT NULL PRIMARY KEY ,disease_name VARCHAR(30) NOT NULL ,
                 subgrp_id VARCHAR(4),FOREIGN KEY(subgrp_id) REFERENCES subgroup(subgrp_id))""")
# Create new table subscriber
mycursor.execute("""CREATE TABLE subscriber (sub_id VARCHAR(10) NOT NULL PRIMARY KEY ,first_name VARCHAR(30) ,
                 last_name VARCHAR(20),street VARCHAR(30),birth_date DATE,gender VARCHAR(6),phone VARCHAR(15),city VARCHAR(30),
                 zip_code int,country VARCHAR(10),subgrp_id VARCHAR(4),elig_ind VARCHAR(2) NOT NULL,eff_date DATE NOT NULL,term_date DATE,FOREIGN KEY(subgrp_id) REFERENCES subgroup(subgrp_id))""")
# create new table hospital_details
mycursor.execute("""CREATE TABLE hospital_details (hospital_id VARCHAR(5) NOT NULL PRIMARY KEY ,hospital_name VARCHAR(255),
                 city VARCHAR(20),state VARCHAR(20),country VARCHAR(6))""")
# Create new table patient_details
mycursor.execute("""CREATE TABLE patient_details(patient_id int NOT NULL PRIMARY KEY,patient_name VARCHAR(20),patient_gender VARCHAR(6),patient_birth_date DATE,patient_phone VARCHAR(15),disease_name VARCHAR(30),city VARCHAR(30),hospital_id VARCHAR(5),FOREIGN KEY(hospital_id) REFERENCES hospital_details(hospital_id))""")

# Create new table claims
mycursor.execute("""CREATE TABLE claims (claim_id int NOT NULL AUTO_INCREMENT PRIMARY KEY ,patient_id int NOT NULL,disease_name VARCHAR(20),
                 sub_id VARCHAR(10),claim_or_rejected VARCHAR(5),claim_type VARCHAR(20),claim_amount float(8,2),claim_date DATE,FOREIGN KEY(sub_id) REFERENCES subscriber(sub_id),FOREIGN KEY(patient_id) REFERENCES patient_details(patient_id))""")
#inserting data into table group 
with open('/home/hadoop/Data/group.csv','r') as file:
    mycursor.execute('''use healthcare''')
    data=file.readlines()
    for row in data:
            list=row.split(",")
            top=tuple([list[0],int(list[1]),int(list[2]),list[3],list[4],list[5],list[6]])
            print(top) 
            sql = "INSERT INTO healthcare.groups_tble(country,premium_written,zip_code,grp_id,grp_name,grp_type,city) VALUES(%s,%s,%s,%s,%s,%s,%s)"
            mycursor.execute(sql, top)
            mydb.commit()

#insert data into subgroup
with open('/home/hadoop/Data/subgroup.csv','r') as file:
    mycursor.execute('''use healthcare''')
    data=file.readlines()
    for row in data:
        list=row.split(",")
        top=tuple([list[0],list[1],float(list[2])])
        print(top) 
        sql = "INSERT INTO healthcare.subgroup(subgrp_id,subgrp_name,monthly_premium) VALUES(%s,%s,%s)"
        mycursor.execute(sql, top)
        mydb.commit()

#inserting data into hospital_details
with open('/home/hadoop/Data/hospital.csv','r') as file:
    mycursor.execute('''use healthcare''')
    data=file.readlines()
    for row in data:
        list=row.split(",")
        top=tuple(list)
        print(top)
        sql = "INSERT INTO hospital_details(hospital_id,hospital_name,city,state,country) VALUES(%s,%s,%s,%s,%s)"
        mycursor.execute(sql,top)
        mydb.commit()

# insert data into table disease
with open('/home/hadoop/Data/disease.csv','r') as file:
    mycursor.execute('''use healthcare''')
    data=file.readlines()
    for row in data:
            list=row.split(",")
            top=tuple([list[0],int(list[1]),list[2]])
            print(top) 
            sql = "INSERT INTO healthcare.disease(subgrp_id,disease_id,disease_name) VALUES(%s,%s,%s)"
            mycursor.execute(sql, top)
            mydb.commit()

#insert data into patient_details table
from datetime import datetime
with open('/home/hadoop/Data/patient.csv','r') as file:
    mycursor.execute('''use healthcare''')
    data=file.readlines()
    for row in data:
            list=row.split(",")
            pdob=list[3]
            pdob=datetime.strptime(pdob,'%Y-%m-%d').date()
            top=tuple([int(list[0]),list[1],list[2],pdob,list[4],list[5],list[6],list[7]])
            print(top) 
            sql = "INSERT INTO healthcare.patient_details(Patient_id,Patient_name,patient_gender,patient_birth_date,patient_phone,disease_name,city,hospital_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
            mycursor.execute(sql, top)
            mydb.commit()

#insert into subscriber
from datetime import datetime
with open('/home/hadoop/Data/subscriber.csv','r') as file:
    mycursor.execute('''use healthcare''')
    data=file.readlines()
    for row in data:
            list=row.split(",")
            print(list)
            pdob1,pdob2,pdob3=list[5],list[13],list[14].strip()
            pdob1=datetime.strptime(pdob1,'%Y-%m-%d').date()
            pdob2=datetime.strptime(pdob2,'%Y-%m-%d').date()
            pdob3=datetime.strptime(pdob3,'%Y-%m-%d').date()
            top=tuple([list[1],list[2],list[3],list[4],pdob1,list[6],list[7],list[8],list[9],int(list[10]),list[11],list[12],pdob2,pdob3])
            print(top)
            
            sql = "INSERT INTO healthcare.subscriber(sub_id,first_name,last_name,street,birth_date,gender,phone,country,city,zip_code,subgrp_id,elig_ind,eff_date,term_date) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            mycursor.execute(sql, top)
            mydb.commit()

# insert data into table group_subgroup
with open('/home/hadoop/Data/grpsubgrp.csv','r') as file:
    mycursor.execute('''use healthcare''')
    data=file.readlines()
    for row in data:
        list=row.split(",")
        top=tuple([list[0],list[1]])
        print(top) 
        sql = "INSERT INTO healthcare.group_subgroup(s_id,g_id) VALUES(%s,%s)"
        mycursor.execute(sql, top)
        mydb.commit()

#insert data into claims 
import pandas as pd 
from datetime import datetime
data = pd.read_json('/home/hadoop/Data/claims.json')
for index,row in data.iterrows():
    b=int(row[1])
    c=row[2]
    d=row[3]
    e=row[4]
    f=row[5]
    g=float(row[6])
    h=row[7]
    h=datetime.strptime(h,'%Y-%m-%d').date()
    val=tuple([b,c,d,e,f,g,h])
    print(val)
    sql="INSERT INTO claims(patient_id,disease_name,sub_id,claim_Or_rejected,claim_type,claim_amount,claim_date) VALUES(%s,%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql,val)
    mydb.commit()

mydb.close()