#Alex Nikolaev
#CNE 340
#This code imports jobs from remotive.com to my wamp server and updates every 4 hours checking for new jobs
# it deletes duplicate entries
# credit goes to christine sutton the CNE 340 class and to Celine for helping me with reinstalling pycharm and helping me with logic errors
# I had to keep the upper case columns upper case the way it came from the fork so I was not able to make them run in lower case even though we discussed doing so for concistency because i would get errors this code now runs perfectly!




import mysql.connector
import time
import json
import requests
from datetime import date
import html2text


def connect_to_sql():
    conn = mysql.connector.connect(user='root', password='',
                                   host='127.0.0.1', database='jobhunter2')
    return conn


def create_tables(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobs (id INT PRIMARY KEY auto_increment, Job_id varchar(100) , 
    company varchar (500), Created_at DATE, url TEXT, Title LONGBLOB, Description LONGBLOB); ''')



def query_sql(cursor, query):
    cursor.execute(query)
    return cursor



def add_new_job(cursor, jobdetails):
    # extract all required columns
    job_id = jobdetails['id']
    company = jobdetails['company_name']
    URL = jobdetails['url']
    title = jobdetails['title']
    description = html2text.html2text(jobdetails['description'])
    date = jobdetails['publication_date'][0:10]
    query = cursor.execute("INSERT INTO jobs( Job_id, company, url, Title, Description, Created_at " ") "
                           "VALUES(%s,%s,%s,%s,%s,%s)", (job_id, company, URL, title, description, date))

    return query_sql(cursor, query)



def check_if_job_exists(cursor, jobdetails):

    query = "SELECT * FROM jobs WHERE Job_id = \"%s\"" % jobdetails['id']
    return query_sql(cursor, query)



def delete_job(cursor, jobdetails):

    query = "DELETE FROM jobs WHERE Job_id = \"%s\"" % jobdetails['id']
    return query_sql(cursor, query)



def fetch_new_jobs():
    query = requests.get("https://remotive.io/api/remote-jobs")
    datas = json.loads(query.text)

    return datas



def jobhunt(cursor):
    # Fetch jobs from website
    print(" Getting you  new jobs!")
    jobpage = fetch_new_jobs()
    add_or_delete_job(jobpage, cursor)


def add_or_delete_job(jobpage, cursor):
    for jobdetails in jobpage['jobs']:
        check_if_job_exists(cursor, jobdetails)
        is_job_found = len(cursor.fetchall()) > 0
        if is_job_found:
                print("this job a repeat!")
                delete_job(cursor, jobdetails)
        else:
            add_new_job(cursor, jobdetails)
            print("New Job Found!!!")


def main():

    conn = connect_to_sql()
    cursor = conn.cursor()
    create_tables(cursor)

    while (1):
        jobhunt(cursor)
        time.sleep(14400)



if __name__ == '__main__':
    main()
