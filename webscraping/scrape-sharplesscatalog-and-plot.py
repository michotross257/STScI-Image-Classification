import time
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd
import astropy.coordinates as coord
from astropy.coordinates import SkyCoord
import astropy.units as units
import matplotlib.pyplot as plt


def extract_from_soup(soup, headers):
    table = pd.DataFrame(columns=headers)
    temp = []
    inc = 0
    for entry in soup.find_all('td')[1:]:
        if len(temp) < len(headers):
            temp.append(entry.text)
        else:
            table.loc[inc, :] = temp
            temp = [entry.text]
            inc += 1
    table.loc[inc, :] = temp
    
    return table

if __name__ == '__main__':
    URL = 'http://www.sharplesscatalog.com/sharplessdata.aspx'
    content = requests.get(URL)
    soup = BeautifulSoup(content.text, features='lxml')
    headers = [i.text.strip() for i in soup.find_all('table')[0].find_all('th')]
    num_of_pages = len([_ for _ in soup.find_all('option')])

    driver = webdriver.Safari()
    driver.get(URL)

    collection = pd.DataFrame(columns=headers)
    for i in range(num_of_pages):
        time.sleep(1)
        driver.find_element_by_id('gvSharplessData_btnNext').click()
        source = driver.page_source
        soup = BeautifulSoup(source, features='lxml')
        table = extract_from_soup(soup, headers)
        collection = pd.concat([collection, table])
    collection.reset_index(drop=True, inplace=True)

    # convert and plot the data
    fig = plt.figure(figsize=(16, 14))
    ax = fig.add_subplot(111, projection="mollweide")
    temp = [SkyCoord(_ra, _dec, unit='deg') for _ra, _dec in zip(collection['RA'],
                                                                 collection['DEC'])]
    ra = coord.Angle([entry.ra for entry in temp])
    ra = ra.wrap_at(180 * units.degree)
    dec = coord.Angle([entry.dec for entry in temp])
    ax.scatter(ra.radian, dec.radian)
    ax.legend()
    plt.show()
