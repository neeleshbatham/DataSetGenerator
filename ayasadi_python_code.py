# CSV Data Generator.
# Author: Neelesh Batham <neelesh.batham007@gmail.com>

# Generate 20 columns with data with 1000000(1M rows)
# Column1: Index Column
# Column2-10: Gaussian Distribution and  10% in each column null.
# Column11-19: Random String from English dictionary and 10% in each column null.
# Column20: Random dates selected between January 1, 2014 to December 31, 2014. No nulls in this column.


import csv
import math
import random
import requests
import sqlite3
from datetime import datetime, timedelta


class DataSetGenerator(object):

    def __init__(self):
        self.fieldnames = ['index', ]
        self.fieldnames += ['col2_20', 'col3_30', 'col4_40',
                            'col5_50', 'col6_60', 'col7_70',
                            'col8_80', 'col9_90', 'col10_100']

        self.fieldnames += ['col11', 'col12', 'col13', 'col14',
                            'col15', 'col16', 'col17', 'col18',
                            'col19']
        self.fieldnames += ['timestamp']

        pass

    def _generate_random_time(self):
        """
        Generates random datetime between the given date.
        :return: datetime
        """
        start_date = datetime(2014, 1, 1)
        end_date = datetime(2014, 12, 31)

        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days
        random_number_of_days = random.randrange(days_between_dates)
        random_date = start_date + timedelta(days=random_number_of_days)
        return random_date

    def _generate_random_words(self):
        """
        Generate list of random words from English Dictionary.
        :return: list of words.
        """

        words_url = "http://svnweb.freebsd.org/csrg/share/dict/words?view=co&content-type=text/plain"
        response = requests.get(words_url)
        return response.content.splitlines()

    def _generate_normal_distribution(self, mean):
        """
        Generate gaussian distribution values.
        :param mean: mean value.
        :return: value
        """
        x = random.randrange(1, 200, 1)
        sd = 1
        var = float(sd) ** 2
        denom = (2 * math.pi * var) ** .5
        num = math.exp(-(float(x) - float(mean)) ** 2 / (2 * var))
        return num / denom

    def load_to_sql(self, file):
        """
        Loads CSV file to SQLite3 DB
        :param file:
        :return:
        """
        con = sqlite3.connect("data.db")  # change to 'sqlite:///your_filename.db'
        cur = con.cursor()
        # create_command = "CREATE TABLE t {0};".format(tuple(self.fieldnames))
        # cur.execute(create_command)

        with open('data.csv', 'r') as f:
            dr = csv.DictReader(f)

            to_db = [(i['index'], i['col2_20'], i['col3_30'], i['col4_40'],
                     i['col5_50'], i['col6_60'], i['col7_70'], i['col8_80'],
                     i['col9_90'], i['col10_100'],

                     i['col11'], i['col12'], i['col13'], i['col14'],
                     i['col15'], i['col16'], i['col17'], i['col18'],
                     i['col19'],
                     i['timestamp']) for i in dr]

        insert_command = "INSERT INTO t {0} VALUES ({1});".format(tuple(self.fieldnames),
                                                                  ','.join('?' * len(self.fieldnames)))

        cur.executemany(insert_command, to_db)
        con.commit()
        con.close()

    def process(self):
        """
        Creates a CSV file.
        :return:
        """
        with open('data.csv', 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            words_list = self._generate_random_words()
            r = random.choice

            writer.writeheader()
            row_limit = 1000000
            null_counter = 0
            null_limit = row_limit*10//100

            for x in range(0, row_limit):
                data = {'index': x,
                        'col2_20': None if x % 3 == 0 and null_counter < null_limit else
                        self._generate_normal_distribution(20),
                        'col3_30': None if x % 2 == 0 and null_counter < null_limit else
                        self._generate_normal_distribution(30),
                        'col4_40': None if x % 3 == 0 and null_counter < null_limit else
                        self._generate_normal_distribution(40),
                        'col5_50': None if x % 2 == 0 and null_counter < null_limit else
                        self._generate_normal_distribution(50),
                        'col6_60': None if x % 3 == 0 and null_counter < null_limit else
                        self._generate_normal_distribution(60),
                        'col7_70': None if x % 2 == 0 and null_counter < null_limit else
                        self._generate_normal_distribution(70),
                        'col8_80': None if x % 3 == 0 and null_counter < null_limit else
                        self._generate_normal_distribution(80),
                        'col9_90': None if x % 2 == 0 and null_counter < null_limit else
                        self._generate_normal_distribution(90),
                        'col10_100': None if x % 3 == 0 and null_counter < null_limit else
                        self._generate_normal_distribution(100),


                        'col11': None if x % 3 == 0 and null_counter < null_limit else r(words_list).decode('utf-8'),
                        'col12': None if x % 2 == 0 and null_counter < null_limit else r(words_list).decode('utf-8'),
                        'col13': None if x % 3 == 0 and null_counter < null_limit else r(words_list).decode('utf-8'),
                        'col14': None if x % 2 == 0 and null_counter < null_limit else r(words_list).decode('utf-8'),
                        'col15': None if x % 3 == 0 and null_counter < null_limit else r(words_list).decode('utf-8'),
                        'col16': None if x % 2 == 0 and null_counter < null_limit else r(words_list).decode('utf-8'),
                        'col17': None if x % 3 == 0 and null_counter < null_limit else r(words_list).decode('utf-8'),
                        'col18': None if x % 2 == 0 and null_counter < null_limit else r(words_list).decode('utf-8'),
                        'col19': None if x % 3 == 0 and null_counter < null_limit else r(words_list).decode('utf-8'),
                        'timestamp': self._generate_random_time()}

                writer.writerow(data)
                null_counter += 1

        return 0


if __name__ == "__main__":

    data_obj = DataSetGenerator()
    data = data_obj.process()
    data_obj.load_to_sql('data.db')
