import time
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
import astropy.coordinates as coord
from astropy.coordinates import SkyCoord
import astropy.units as units
import matplotlib.pyplot as plt


def extract_from_soup(soup, headers, start_index=0):
    '''Extracts table cell values from BeautifulSoup object and insert the
    data into a Pandas DataFrame.'''
    table = pd.DataFrame(columns=headers)
    temp = []
    inc = 0
    for entry in soup.find_all('td')[start_index:]:
        if len(temp) < len(headers):
            temp.append(entry.text)
        else:
            table.loc[inc, :] = temp
            temp = [entry.text]
            inc += 1
    table.loc[inc, :] = temp
    
    return table


def get_tables(url):
    '''Scrape the page for all tables.'''
    content = requests.get(url)
    soup = BeautifulSoup(content.text, features='lxml')
    headers = [re.sub(pattern='\[\d\]',
                      repl='',
                      string=i.text.strip()) for i in soup.find_all('table')[0].find_all('th')]

    tables = {}
    for cnt, table in enumerate(soup.find_all('table')):
        key = re.sub(pattern='\[\w+\]',
                     repl='',
                     string=table.findPreviousSibling().text)
        tables[key] = extract_from_soup(table, headers)
    
    return tables


if __name__ == '__main__':
    # ==============================
    # WIKIPEDIA
    # ------------------------------
    # Scrape the tables on two pages
    # ==============================

    clusters = get_tables('https://en.wikipedia.org/wiki/List_of_Abell_clusters')
    nebulae = get_tables('https://en.wikipedia.org/wiki/List_of_star-forming_regions_in_the_Local_Group')

    # for each page, aggregate the different tables
    _clusters = pd.DataFrame()
    _nebulae = pd.DataFrame()
    for key in clusters:
        _clusters = pd.concat([_clusters, clusters[key]])
    for key in nebulae:
        _nebulae = pd.concat([_nebulae, nebulae[key]])
    _clusters.rename(columns={'Right ascension (J2000)': 'RA',
                              'Declination (J2000)': 'DEC'}, inplace=True)
    _nebulae.rename(columns={'RA [deg]': 'RA',
                             'Dec [deg]': 'DEC'}, inplace=True)

    classes = {'cluster': _clusters, 'nebula': _nebulae}

    # ===================================
    # SHARPLESS CATALOG
    # -----------------------------------
    # Scrape from tables that require the
    # user to navigate to each table by
    # clicking a [NEXT] button
    # ===================================

    URL = 'http://www.sharplesscatalog.com/sharplessdata.aspx'
    content = requests.get(URL)
    soup = BeautifulSoup(content.text, features='lxml')
    headers = [i.text.strip() for i in soup.find_all('table')[0].find_all('th')]
    num_of_pages = len([_ for _ in soup.find_all('option')])

    driver = webdriver.Safari()
    driver.get(URL)

    # step through each page by clicking the [Next] button
    sharpless_collection = pd.DataFrame(columns=headers)
    for i in range(num_of_pages):
        time.sleep(1)
        driver.find_element_by_id('gvSharplessData_btnNext').click()
        source = driver.page_source
        soup = BeautifulSoup(source, features='lxml')
        table = extract_from_soup(soup, headers, start_index=1)
        sharpless_collection = pd.concat([sharpless_collection, table])
    sharpless_collection.reset_index(drop=True, inplace=True)

    # ====================
    # Plot all of the data
    # ====================

    fig = plt.figure(figsize=(16, 14))
    ax = fig.add_subplot(111, projection="mollweide")

    for cls in classes:
        ra = classes[cls]['RA']
        dec = classes[cls]['DEC']
        if cls == 'cluster':
            # \xa0 is the non-breaking space in Latin1 (ISO 8859-1)
            # it is necessary to replace this character
            temp = [SkyCoord(_ra.replace('\xa0', ' ').strip(),
                             _dec.replace('\xa0', ' ').strip()) for _ra, _dec in zip(ra, dec)]
        else:
            ra = ra.append(sharpless_collection['RA'])
            dec = dec.append(sharpless_collection['DEC'])
            temp = [SkyCoord(_ra, _dec, unit='deg') for _ra, _dec in zip(ra, dec)]
        ra = coord.Angle([entry.ra for entry in temp])
        ra = ra.wrap_at(180 * units.degree)
        dec = coord.Angle([entry.dec for entry in temp])
        ax.scatter(ra.radian, dec.radian, label=cls)
    ax.legend()
    plt.show()
