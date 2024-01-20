import time
from datetime import datetime, timezone
from datetime import date
import pytz
import pandas as pd
from pytrends.request import TrendReq
import psycopg2

# Connect to Google
pytrends = TrendReq(hl='en-US', tz=360)

# to estabilish connection between DB
conn = psycopg2.connect(
    dbname="assetscan_qa",
    user="postgres",
    password="7359",
    host="localhost",
    port="5432"
)
# to modify DB cursor
cur = conn.cursor()
cur.execute('SELECT location_name, id FROM public.locality ORDER BY id ASC')
# Fetch all rows from the executed query
rows = cur.fetchall()

# Create empty lists to store values from each column
kw_list = []
location_id_list = []

# Iterate through the rows and add values from each column into their respective lists
for row in rows:
    kw_list.append(row[0])  # Add values from location_name to column1_list
    location_id_list.append(row[1])  # Add values from column 2 to column2_list

print(kw_list)
print(location_id_list)

start_date = '2018-01-01'
end_date = date.today().strftime("%Y-%m-%d")
timeframe = start_date + " " + end_date
cat = '29'  # 29 for property


# # below function to get data from google trends
def fetch_trends_data(keyword):
    time.sleep(8)
    pytrends.build_payload([keyword],
                           cat,
                           timeframe,
                           geo='',
                           gprop=''
                           )
    time.sleep(3)
    return pytrends.interest_over_time()


# iterating over fetched data to add into CSV and to push into DB
for keyword in range(3):
    suggestions = pytrends.suggestions(kw_list[keyword])
    topic_id = None
    for suggestion in suggestions:
        if suggestion['title'] == kw_list[keyword] and suggestion['type'] == 'Topic':
            topic_id = suggestion['mid']
            break

    success = False
    while not success:
        try:
            data = None
            row_name = None
            if topic_id:
                data = fetch_trends_data(topic_id)
                if not data.empty:
                    row_name = topic_id
                    print(data)
                else:
                    data = fetch_trends_data(kw_list[keyword])
                    row_name = kw_list[keyword]
                    print(data)

            else:
                data = fetch_trends_data(kw_list[keyword])
                row_name = kw_list[keyword]
                print(data)

            success = True
        except Exception as e:
            print(e)
            pass
            # # # for datbase inserting
    month_list=[]
    avg_value_list=[]
    for idx, row in data.iterrows():
        #     # print(idx.strftime('%Y-%m-%d'), keyword, row[keyword] )

        # month_value = pd.to_datetime(idx).strftime('%y-%m-%d %H:%M:%S %z')
        # start_date1 = datetime.strptime(start_date, '%Y-%m-%d')
        # start_date1 = pytz.timezone('utc').localize(start_date1)
        # end = datetime.now()
        # created_date = datetime.now()

        # end=datetime.now(timezone.utc)

        month = datetime.strftime(idx, '%d-%m-%y')
        month_list.append(month)
        avg_value_list.append(int(row[row_name]))
        # print('month',month)
        # print('end',end)
        # print('start_date1',start_date1,'created-date',created_date)

        # print(int(location_id_list[keyword]), month, int(row[row_name]), created_date, start_date1, end)
    moving_avg=[]
    for avg in range(len(avg_value_list)-2):
        summ=0
        for j in range(3):
            summ+=avg_value_list[avg+j]

        a=max(summ/3,20)
        # moving_avg.append(a)
        start_date1 = datetime.strptime(start_date, '%Y-%m-%d')
        start_date1 = pytz.timezone('utc').localize(start_date1)
        end = datetime.now()
        created_date = datetime.now()

        # print(int(location_id_list[keyword]), month_list[avg+2],round(a), created_date, start_date1, end)


        insert_script = (
            'INSERT INTO public.google_trends(location_id, months, value, created_date, start_date, end_date) VALUES(%s,%s,%s,%s,%s,%s)')
        insert_values = (
            int(location_id_list[keyword]),month_list[avg+2],round(a), created_date, start_date1, end)
        cur.execute(insert_script, insert_values)
    conn.commit()
# need to commit ,close cursor and DB connection
cur.close()
conn.close()




# # Save to a CSV file
#
# # def check_trends():
# #     pytrends.build_payload(kw,
# #                             cat,
# #                             timeframe='2018-01-01 2023-12-22',
# #                             geo='',
# #                             gprop=''
# #                           )
# #
# #     iot = pytrends.interest_over_time()
# #     print(iot)
# # for i in kw_list:
# #     kw.append(i)
# #     check_trends()
# #     kw.pop()
#
# # combined_data.to_csv('trend_data.csv')
#
# # j=iot['date','Kokapet: (Worldwide)']
# # Save to a CSV file
# # iot.to_csv('madhapur_trends.csv')


# import time
# from openpyxl import load_workbook
# from selenium import webdriver
# from selenium.webdriver import Keys, ActionChains
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
#
# options = Options()
# options.add_experimental_option("detach", True)
# driver = webdriver.Chrome(options=options)
# driver.maximize_window()
# driver.implicitly_wait(10)
# # ------------------------------------------------------------------------------------------------------------------------------------
# driver.get("https://trends.google.com/trends/")
# time.sleep(5)
# listt = [ 'Kokapet']
# //mail login
# # driver.find_element(By.XPATH, "//span[normalize-space()='Sign in']").click()
# # time.sleep(2)
# # driver.find_element(By.XPATH, "//*[@id='identifierId']").send_keys("abzal.hussain@assetmonk.com")
# # driver.find_element(By.XPATH, "//span[normalize-space()='Next']").click()
# # time.sleep(3)
# # driver.find_element(By.XPATH, "//input[@type='password']").send_keys("73597359@Abzal")
# # driver.find_element(By.XPATH, "//span[normalize-space()='Next']").click()
# # time.sleep(7)
# for i in listt:
#     name = i  # Kukatpally,Madhapur,Kokapet
#
#     driver.find_element(By.XPATH, "//input[@id='i7']").clear()
#     for j in i:
#         driver.find_element(By.XPATH, "//input[@id='i7']").send_keys(j)
#
#     # driver.find_element(By.XPATH, "//input[@id='i7']").send_keys(name,Keys.SPACE)
#     time.sleep(1)
#     actions = ActionChains(driver)
#     actions.key_down(Keys.SPACE)
#     # driver.find_element(By.XPATH, "//input[@id='i7']").send_keys(Keys.SPACE)
#     time.sleep(3)
#
#     # //div[@role='main']//li[1]//a[1]/div/div
#
#     l = len(driver.find_elements(By.XPATH, "//div[@role='main']//li//a[1]"))
#     validator = []
#     for r in range(1, l + 1):
#
#         # //div[@role='main']//li[{i}]//a[1]/div/div[normalize-space()='{name}']
#         nm = driver.find_element(By.XPATH,
#                                  f"(//div[@role='main']//li[{r}]//a[1]/div/div)").text.replace(" ", "").lower()
#
#         if nm == name.replace(" ", "").lower():
#
#             #             #//div[@role='main']//li[{i + 1}]//a[1]/div/div[contains(text(),'Topic')]
#
#             topic = driver.find_element(By.XPATH,
#                                         f"(//div[@role='main']//li[{r}]//a[1]/div/div)[2]").text
#             if topic == "Topic":
#                 validator.append(True)
#                 driver.find_element(By.XPATH,
#                                     f"(//div[@role='main']//li[{r}]//a[1]/div/div)[2]").click()
#
#                 #
#                 #                 # e=driver.find_element(By.XPATH,
#                 #                 #                     f"(//div[@role='main']//li[{i}]//a[1]/div/div)[2]")
#                 #                 # driver.execute_script("arguments[0].click()",e)
#                 break
#             else:
#                 validator.append(False)
#     if True not in validator:
#         for k in range(1, l + 1):
#
#             # //div[@role='main']//li[{i}]//a[1]/div/div[normalize-space()='{name}']
#             nm = driver.find_element(By.XPATH,
#                                      f"(//div[@role='main']//li[{k}]//a[1]/div/div)").text
#
#             if nm == name:
#
#                 #             #//div[@role='main']//li[{i + 1}]//a[1]/div/div[contains(text(),'Topic')]
#
#                 topic = driver.find_element(By.XPATH,
#                                             f"(//div[@role='main']//li[{k}]//a[1]/div/div)[2]").text
#                 if topic == "Search term":
#                     driver.find_element(By.XPATH,
#                                         f"(//div[@role='main']//li[{k}]//a[1]/div/div)[2]").click()
#
#                     break
#
#     time.sleep(3)
#
#     #compare location adding
#     driver.find_element(By.XPATH,"//span[normalize-space()='Compare']").click()
#     time.sleep(1)
#     comparison='Madhapur'
#     driver.find_element(By.XPATH,"(//input[@placeholder='Add a search term'])[2]").send_keys(comparison)
#     time.sleep(1)
#     # Find the element where you want to simulate pressing the down arrow key
#     target_element = driver.find_element(By.XPATH,"(//input[@placeholder='Add a search term'])[2]")
#     target_element.send_keys(Keys.ARROW_DOWN + Keys.ARROW_DOWN+Keys.ENTER)
#
#
#
#
#     driver.find_element(By.XPATH, "//hierarchy-picker[@data='ctrl.geoOptions']//ng-include").click()
#     time.sleep(2)
#     driver.find_element(By.XPATH,
#                         "//span[@md-highlight-text='ctrl.searchQuery'][normalize-space()='Worldwide']").click()
#     time.sleep(2)
#     #Date range set up
#     driver.find_element(By.XPATH, "(//div[contains(text(),'Past day')])[1]").click()
#     time.sleep(1)
#     range=driver.find_element(By.XPATH,"//div[normalize-space()='Custom time range...']")
#     driver.execute_script("arguments[0].click()",range)
#     time.sleep(2)
#     driver.find_element(By.XPATH, "(//input[@class='md-datepicker-input'])[1]").clear()
#     driver.find_element(By.XPATH,"(//input[@class='md-datepicker-input'])[1]").send_keys('1/1/2018')
#     # driver.find_element(By.XPATH, "(//input[@class='md-datepicker-input'])[2]").clear()
#     # driver.find_element(By.XPATH, "(//input[@class='md-datepicker-input'])[2]").send_keys('12/22/2023')
#     time.sleep(2)
#     driver.find_element(By.XPATH,"//button[normalize-space()='OK']").click()
#     time.sleep(3)
#
#
#     # driver.find_element(By.XPATH, "(//div[normalize-space()='Past 5 years'])[1]").click()
#     # time.sleep(2)
#     driver.find_element(By.XPATH, "//hierarchy-picker[@data='ctrl.categoryOptions']//ng-include").click()
#     time.sleep(3)
#     # input
#     driver.find_element(By.ID, "input-12").send_keys('Real', Keys.SPACE)
#     time.sleep(2)
#     zz=driver.find_element(By.XPATH, "(//span[contains(text(),'Real')])[1]")
#     driver.execute_script("arguments[0].click()",zz)
#     time.sleep(2)
#
#     graph_or_warn = driver.find_elements(By.XPATH, "//ng-include[contains(@src,'/views/charts.html')] ")
#     if len(graph_or_warn) > 0:
#         download = driver.find_element(By.XPATH, "(//i[contains(text(),'file_download')])[1]").is_displayed()
#         if download:
#             driver.find_element(By.XPATH, "(//i[contains(text(),'file_download')])[1]").click()
#         else:
#             driver.refresh()
#             time.sleep(3)
#     else:
#         driver.find_element(By.XPATH, "(//a[normalize-space()='Home'])[1]").click()
#         time.sleep(3)
#         # repeat
#         driver.find_element(By.XPATH, "//input[@id='i7']").clear()
#         for j in i:
#             driver.find_element(By.XPATH, "//input[@id='i7']").send_keys(j)
#
#         # driver.find_element(By.XPATH, "//input[@id='i7']").send_keys(name,Keys.SPACE)
#         time.sleep(1)
#         actions = ActionChains(driver)
#         actions.key_down(Keys.SPACE)
#         # driver.find_element(By.XPATH, "//input[@id='i7']").send_keys(Keys.SPACE)
#         time.sleep(3)
#         # search term click
#         driver.find_element(By.XPATH,
#                             f"(//div[@role='main']//li[1]//a[1]/div/div)").click()
#         time.sleep(3)
#         driver.find_element(By.XPATH, "//hierarchy-picker[@data='ctrl.geoOptions']//ng-include").click()
#         time.sleep(2)
#         driver.find_element(By.XPATH,
#                             "//span[@md-highlight-text='ctrl.searchQuery'][normalize-space()='Worldwide']").click()
#         time.sleep(2)
#         driver.find_element(By.XPATH, "(//div[contains(text(),'Past day')])[1]").click()
#         time.sleep(1)
#
#         driver.find_element(By.XPATH, "(//div[normalize-space()='Past 5 years'])[1]").click()
#         time.sleep(2)
#         driver.find_element(By.XPATH, "//hierarchy-picker[@data='ctrl.categoryOptions']//ng-include").click()
#         # input
#         driver.find_element(By.XPATH, "(//input[@type='search'])[3]").send_keys('Real', Keys.SPACE)
#         time.sleep(2)
#         driver.find_element(By.XPATH, "(//span[contains(text(),'Real')])[1]").click()
#         time.sleep(2)
#
#         dnd = driver.find_element(By.XPATH, "(//i[contains(text(),'file_download')])[1]")
#         download = dnd.is_displayed()
#         if download:
#             driver.execute_script("arguments[0].click()", dnd)
#             # driver.find_element(By.XPATH, "(//i[contains(text(),'file_download')])[1]").click()
#         else:
#             driver.refresh()
#             time.sleep(3)
#
#     # click to home
#     time.sleep(3)
#     driver.find_element(By.XPATH, "(//a[normalize-space()='Home'])[1]").click()
#     time.sleep(3)
#
# # driver.quit()


# *//ng-include[contains(@src,"/views/charts.html")]
# *//ng-include[contains(@src,"/views/error_message.html")]
# (//div[contains(text(),'Topic')])[1]
# 17.40012888157839, 78.56019334461321
# /html/body/div[2]/div[2]/div/md-content/div/div/div[1]/trends-widget/ng-include/widget/div/div/ng-include/div/ng-include/div/line-chart-directive/div[1]/div/div[1]/div/div/table/tbody/tr[1]
