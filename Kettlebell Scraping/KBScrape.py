import pandas as pd
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import time
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pygsheets
from googleapiclient import discovery
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from tabulate import tabulate
import re
import requests
from bs4 import BeautifulSoup


class KBScraping(object):
    def __int__(self, number_of_scrapes, time_between_scrapes, email_to, email_from, password):
        self.company_dictionary = {'rep_fitness': 'https://www.repfitness.com/conditioning/strength-equipment/kettlebells/rep-kettlebells',
                                        'american_barbell': 'https://americanbarbell.com/products/american-barbell-cast-kettlebell?variant=988734644',
                                        'rogue': 'https://www.roguefitness.com/rogue-kettlebells',
                                        'kb_kings_kg': 'https://www.kettlebellkings.com/powder-coat-kettlebell/',
                                        'kb_kings_lb': 'https://www.kettlebellkings.com/powder-coat-kettlebell-in-lb/',
                                        'onnit': 'https://www.onnit.com/onnit-kettlebells/'}
        self.google_file = google_file
        self.email_password = password
        self.email_to = email_to
        self.email_from = email_from

        download_dir = ('file_path')
        self.chrome_options = Options()
        self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument('window-size=1920,1920')
        self.chrome_options.add_argument("--auto-open-devtools-for-tabs")
        self.chrome_options.add_experimental_option('prefs', {"download.default_directory": download_dir,
                                                              "download.prompt_for_download": False,
                                                              "download.directory_upgrade": True,
                                                              "plugins.always_open_pdf_externally": True
                                                              }
                                                    )
        self.driver_location = driver_location
        self.google_creds = self.google_creds
        self.number_of_scrapes = number_of_scrapes
        self.time_between_scrapes = time_between_scrapes
        self.rep_fitness_inventory = None
        self.rogue_df = None
        self.american_barbell_inventory = None
        self.kb_kings_kg_df = None
        self.kb_kings_lb_df = None
        self.onnit_df = None
        self.old_scraped_df = None
        self.combined_df = None
        self.email_df = None
        self.exclude_products = ['Kettlebell Storage Rack',
                                 'REP Kettlebell - 1 kg',
                                 'Choose Options',
                                 'Onnit Kettlebell Starter']

    def run_scrape(self):
        print('Starting...')
        for i in range(0, self.number_of_scrapes):
            driver = webdriver.Chrome(self.driver_location,
                                      options=self.chrome_options)
            driver.set_window_size(1920, 1920)

            self.rep_fitness()
            self.rogue()
            self.american_barbell()
            self.kb_kings_kg(driver=driver)
            self.kb_kings_lb(driver=driver)
            self.onnit(driver=driver)
            self.create_new_df()

            self.read_spreadsheet()

            self.create_email_df()

            self.send_email()
            self.edit_spreadsheet()



            driver.quit()
            time.sleep(self.time_between_scrapes)

        print('...done')

    def rep_fitness(self):

        out_of_stock_str = 'Subscribe to back in stock notification Subscribe Out of stock'

        print('Trying Rep Fitness')

        url = self.company_dictionary['rep_fitness']
        page = requests.get(url)

        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find(id='super-product-table')

        self.rep_fitness_inventory = pd.read_html(str(table))[0]

        self.rep_fitness_inventory['company'] = 'Rep Fitness'
        self.rep_fitness_inventory['website'] = url
        self.rep_fitness_inventory['is_instock'] = self.rep_fitness_inventory['Qty'].apply(lambda x: 'False' if x == out_of_stock_str else 'True')

        self.rep_fitness_inventory.rename(columns={'Product Name': 'product', 'Price': 'price'},
                                          inplace=True)
        self.rep_fitness_inventory.drop(columns=['Qty'],
                                        inplace=True)
        self.rep_fitness_inventory = self.rep_fitness_inventory[['company',
                                                                 'is_instock',
                                                                 'price',
                                                                 'product',
                                                                 'website']]

    def rogue(self):

        print('Trying Rogue')
        rogue_inventory = []
        url = self.company_dictionary['rogue']
        page = requests.get(url)

        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find(id='super-product-table')

        in_stock_list = []

        for item in table.findAll('div', class_="item-qty input-text"):

            value = item
            list_obj = value.findAll('input',class_='input-text qty form-control')
            text = str(list_obj[0])
            result_grpd = re.search('id="grouped-product-item-(.*)" max', text)
            result = result_grpd.group(1)

            result_str = 'grouped-item product-purchase-wrapper-'+result

            in_stock_list.append(result_str)

        for item in in_stock_list:

            table_object = table.find('div', class_= item)

            rogue_inventory.append({'company': 'Rogue',
                                    'product': table_object.find('div',class_="item-name").text,
                                    'price': table_object.find('span',class_="price").text,
                                    'is_instock': 'True',
                                    'website': url})

        self.rogue_df = pd.DataFrame(rogue_inventory)

    def american_barbell(self):

        print('Trying American Barbell')

        url = self.company_dictionary['american_barbell']
        page = requests.get(url)

        soup = BeautifulSoup(page.content, 'html.parser')
        table = soup.find(id='prb_product_table')

        self.american_barbell_inventory = pd.read_html(str(table))[0]

        self.american_barbell_inventory['company'] = 'American Barbell'
        self.american_barbell_inventory['website'] = url
        self.american_barbell_inventory['is_instock'] = 'True'

        self.american_barbell_inventory.rename(columns={'Product': 'product', 'Price': 'price'},
                                               inplace=True)
        self.american_barbell_inventory.drop(columns=['Qty','Sku'],
                                             inplace=True)
        self.american_barbell_inventory = self.american_barbell_inventory[['company',
                                                                           'is_instock',
                                                                           'price',
                                                                           'product',
                                                                           'website']]

    def kb_kings_kg(self, driver):

        print('Trying KB Kings KG')
        kb_kings_inventory = []
        url = self.company_dictionary['kb_kings_kg']
        driver.get(url)
        time.sleep(5)

        drop_down = driver.find_element_by_id('attribute_select_886')
        item = []
        for i in drop_down.find_elements_by_tag_name("option"):
            item.append(i)

        print('     ...looping table')

        for i in item:
            time.sleep(2)
            i.click()
            time.sleep(2)

            page = driver.page_source
            soup = BeautifulSoup(page, 'html.parser')
            product = i.text
            price = soup.find('span', class_= 'price price--withoutTax').text
            if driver.find_element_by_id('form-action-addToCart').is_enabled():
                is_instock = 'True'
            else:
                is_instock = 'False'

            kb_kings_inventory.append({'company': 'KB Kings',
                                       'product': product,
                                       'price': price,
                                       'is_instock': is_instock,
                                       'website': url})

        self.kb_kings_kg_df = pd.DataFrame(kb_kings_inventory)

    def kb_kings_lb(self, driver):

        kb_kings_inventory = []

        print('Trying KB Kings LB')

        url = self.company_dictionary['kb_kings_lb']
        driver.get(url)
        time.sleep(5)

        drop_down = driver.find_element_by_id('attribute_select_887')
        item = []
        for i in drop_down.find_elements_by_tag_name("option"):
            item.append(i)

        print('     ...looping table')

        for i in item:
            time.sleep(2)
            i.click()
            time.sleep(2)

            page = driver.page_source
            soup = BeautifulSoup(page, 'html.parser')
            product = i.text
            price = soup.find('span', class_='price price--withoutTax').text
            if driver.find_element_by_id('form-action-addToCart').is_enabled():
                is_instock = 'True'
            else:
                is_instock = 'False'

            kb_kings_inventory.append({'company': 'KB Kings',
                                       'product': product,
                                       'price': price,
                                       'is_instock': is_instock,
                                       'website': url})

        self.kb_kings_lb_df = pd.DataFrame(kb_kings_inventory)

    def onnit(self, driver):

        onnit_inventory = []

        print('Trying Onnit')

        url = self.company_dictionary['onnit']
        driver.get(url)
        time.sleep(5)

        drop_down = driver.find_element_by_xpath('//*[@id="main-select"]')
        item = []
        for i in drop_down.find_elements_by_tag_name("option"):
            item.append(i)

        print('     ...looping table')

        for i in item:
            time.sleep(2)
            i.click()
            time.sleep(2)

            product_all = i.text
            product = " ".join(product_all.split(" ", 3)[:3])
            stock = " ".join(product_all.split(" ", 3)[-1:])
            price = driver.find_element_by_xpath('//*[@id="buy-now"]/div/div[2]/div[1]/div[1]/span').text

            if stock == '(back in stock soon)':
                is_instock = 'False'
            else:
                is_instock = 'True'

            onnit_inventory.append({'company': 'Onnit',
                                    'product': product,
                                    'price': price,
                                    'is_instock': is_instock,
                                    'website': url})

        self.onnit_df = pd.DataFrame(onnit_inventory)

    def send_email(self):

        if len(self.email_df) > 0:

            fromaddr = self.email_to
            toaddr = self.email_from

            text = """
            Hello,
    
            Here are the new Kettlebells in stock:
    
            {table}
    
            Best,
    
            Name"""

            html = """
            <html>
            <head>
            <style> 
             table, th, td {{ border: 1px solid black; border-collapse: collapse; }}
              th, td {{ padding: 5px; }}
            </style>
            </head>
            <body><p>Hello,</p>
            <p>Here are the new Kettlebells in stock:</p>
            {table}
            <p>Best,</p>
            <p>Chris</p>
            </body></html>
            """

            col_list = list(self.email_df.columns.values)
            data = self.email_df
            text = text.format(table=tabulate(data, headers=col_list, tablefmt="grid"))
            html = html.format(table=tabulate(data, headers=col_list, tablefmt="html"))

            msg = MIMEMultipart(
                "alternative", None, [MIMEText(text), MIMEText(html, 'html')])


            msg['From'] = fromaddr
            msg['To'] = toaddr

            msg['Subject'] = 'New Kettlebell Inventory'

            send_to = toaddr
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.ehlo()
            server.starttls()
            server.login(fromaddr, password=self.email_password)
            server.sendmail(fromaddr, send_to, msg.as_string())
            server.quit()

        else:
            pass

    def read_spreadsheet(self):
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.google_creds,
            scope)
        client = gspread.authorize(creds)
        file = client.open_by_url(
            self.google_file)
        ws1 = file.get_worksheet(0)
        records = ws1.get_all_records()
        df_old = pd.DataFrame(records)
        self.old_scraped_df = df_old[['company',
                                      'is_instock',
                                      'product',
                                      'website',
                                      'scrape_time']]

    def create_new_df(self):

        self.combined_df = pd.concat([self.rep_fitness_inventory,
                                      self.rogue_df,
                                      self.american_barbell_inventory,
                                      self.kb_kings_kg_df,
                                      self.kb_kings_lb_df,
                                      self.onnit_df],
                                     ignore_index=True)

        self.combined_df['scrape_time'] = str(datetime.now())
        self.combined_df = self.combined_df.query('is_instock == "True"')
        self.combined_df = self.combined_df[~self.combined_df['product'].isin(self.exclude_products)]
        self.combined_df.rename(columns={'price':'price - shipping may or may not be included'},
                                inplace=True)

    def create_email_df(self):

        df_all = self.combined_df.merge(self.old_scraped_df.drop_duplicates(),
                                        on=['company', 'product'],
                                        how='left',
                                        indicator=True)

        self.email_df = df_all.query('_merge == "left_only"')

        self.email_df.drop(columns=['is_instock_y','website_y','scrape_time_y','_merge'],
                           inplace=True)
        self.email_df.rename(columns={'is_instock_x':'is_instock',
                                      'website_x':'website',
                                      'scrape_time_x':'scrape_time'
                                      },
                             inplace=True)

    def edit_spreadsheet(self):
        # Clear Spreadsheet
        rangeAll = '{0}!A1:Z'.format('Inventory List')
        body = {}

        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.google_creds,
            scope)
        service = discovery.build('sheets', 'v4', credentials=creds)
        service.spreadsheets().values().clear(spreadsheetId='13d5XKMeO0x8-tenUm9bziDdJ6ACrCZmw-EXZf-EypQI',
                                              range=rangeAll,
                                              body=body).execute()
        # Update Spreadsheet
        gc = pygsheets.authorize(service_file=self.google_creds)
        file = gc.open_by_url(
            self.google_file)
        wks = file[0]
        wks.set_dataframe(self.combined_df, 'A1')
