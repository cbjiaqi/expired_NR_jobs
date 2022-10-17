from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import argparse

# spark2-submit email-onet-locs-smry.py --db dataservices --email madhav.sigdel@careerbuilder.com,jiaqi.xu@careerbuilder.com

parser = argparse.ArgumentParser()
parser.add_argument('--db', help='source database')
parser.add_argument('--email', help='to_email ids')
args = parser.parse_args()
db = args.db.strip().lower()
to_email: str = args.email

def send_email(from_email, to_email, subject, html_message):
    import smtplib

    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email

    part = MIMEText(html_message, 'html')
    msg.attach(part)

    s = smtplib.SMTP('localhost')
    s.sendmail(from_email, to_email.split(','), msg.as_string())
    s.quit()

def html_part_data(title, df):
    df_html = df.style \
        .set_table_styles([{'selector': 'th', 'props': [('font-size', '11pt')]}]) \
        .set_properties(
        **{'font-size': '9pt', 'border-color': 'black', 'background-color': '#F0FFFF', 'border-style': 'solid',
           'border-width': '1px', 'border-collapse': 'collapse'}) \
        .render()

    html = f"""<h2 style = "text-align: left;" > {title} </h2>
    {df_html}
    <p>"""
    return html

sc = SparkContext(conf=SparkConf().setAppName('DS-check-expired-NR-jobs-vs-Hive-email'))
sqlContext = HiveContext(sc)

sql = f"""
select * from {db}.qqq_check_rec_jobs_new_relic_gbr_scale_ten
limit 10
"""

df = sqlContext.sql(sql).toPandas()
email_text = html_part_data('2 hours example for expired NR jobs GBR SCALE TEN', df)

sql = f"""
select * from {db}.qqq_check_rec_jobs_new_relic_gbr_mix_a
limit 10
"""

df = sqlContext.sql(sql).toPandas()
email_text += html_part_data('2 hours example for expired NR jobs GBR MIX A', df)

sql = f"""
select * from {db}.qqq_check_rec_jobs_new_relic_gbr_mix_b
limit 10
"""

df = sqlContext.sql(sql).toPandas()
email_text += html_part_data('2 hours example for expired NR jobs GBR MIX B', df)

sql = f"""
select * from {db}.qqq_check_rec_jobs_new_relic_gbr_mix_c
limit 10
"""

df = sqlContext.sql(sql).toPandas()
email_text += html_part_data('2 hours example for expired NR jobs GBR MIX C', df)

from_email = "ds-no-reply@careerbuilder.com"
subject = 'Expired Jobs in NR vs Hive.'
send_email(from_email, to_email, subject, email_text)