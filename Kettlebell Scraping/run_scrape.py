from KBScrape import KBScraping

kb_scraping = KBScraping(number_of_scrapes=20,
                         time_between_scrapes=1800,
                         email_to='dummy@dummy.com',
                         email_from='dummy@dummy.com',
                         password='email_password')

kb_scraping.run_scrape()
