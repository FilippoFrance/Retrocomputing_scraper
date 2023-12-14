import aiohttp
import asyncio
from bs4 import BeautifulSoup
import logging, datetime
import sqlite3


# Classe per la gestione del database
class ComputerDatabase:

    def __init__(self, db_name='retrocomputing_database.db'):
        # Inizializzazione della connessione al database
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        # Creazione delle tabelle nel database
        self.create_tables()

    def create_tables(self):
        # Crea la tabella device
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS device (
                manufacturer TEXT,
                name TEXT,
                type TEXT,
                origin TEXT,
                year TEXT,
                quantity_built TEXT,
                cpu TEXT,
                ram TEXT,
                rom TEXT,
                io_ports TEXT,
                price TEXT,
                PRIMARY KEY (manufacturer, name)
            )
        ''')

        # Crea la tabella images
        self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS image (
                    img_link TEXT PRIMARY KEY,
                    caption TEXT,
                    name TEXT,
                    manufacturer TEXT,
                    FOREIGN KEY (name, manufacturer) REFERENCES device(name, manufacturer)
                )
        ''')

        # Conferma le modifiche al database
        self.conn.commit()

    def insert_data(self, device_data):
        # Estrae i dati del dispositivo e delle immagini dal dizionario
        device = device_data['DEVICE']
        images = device_data['IMAGES']

        # Inserisci dati nella tabella device
        self.cursor.execute('''
                INSERT OR REPLACE INTO device (
                    manufacturer, name, type, origin, year, quantity_built, price, cpu, ram, rom, io_ports
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
            device.get('MANUFACTURER', None),
            device.get('NAME', None),
            device.get('TYPE', None),
            device.get('ORIGIN', None),
            device.get('YEAR', None),
            device.get('QUANTITY BUILT', None),
            device.get('PRICE', None),
            device.get('CPU', None),
            device.get('RAM', None),
            device.get('ROM', None),
            device.get('I/OPORTS', None)
        ))

        # Inserisci dati nella tabella images
        for image in images:
            self.cursor.execute('''
            INSERT OR IGNORE INTO image (
                img_link, caption, name, manufacturer
            ) VALUES (?, ?, ?, ?)
            ''', (
                image.get('img_link'), image.get('caption'), device.get('name'), device.get('manufacturer')))

        # Conferma le modifiche al database
        self.conn.commit()

    def close_connection(self):
        # Chiude la connessione al database
        self.conn.close()

    def insertion(self, results):
        # Crea un'istanza del gestore del database
        db_handler = ComputerDatabase()
        for entry in results:
            # Inserisce i dati nel database
            db_handler.insert_data(entry)
        # Chiude la connessione al database
        db_handler.close_connection()


# Classe per l'estrazione dei dati
class ExtractionHandler:

    # Funzione per estrarre i valori da una tabella HTML
    async def get_data_from_table(self, table_data):
        device = {}

        # Estrazione dei dati dalla tabella
        for row in table_data.find_all('tr'):
            columns = row.find_all('td')

            # Estrae il valore dalla colonna 0 e lo utilizza come chiave
            key = columns[0].text.strip().replace('\xa0', '')
            value = columns[1].text.strip().replace('\xa0', '')
            device[key] = value

            try:
                # Prova a estrarre il valore dalla colonna 2 e lo aggiunge al dizionario
                key = columns[2].text.strip().replace('\xa0', '')
                value = columns[3].text.strip().replace('\xa0', '')
                device[key] = value
            except:
                pass  # Ignora il caso in cui non ci sono ulteriori colonne

        return device


# Classe per la gestione delle richieste asincrone
class RequestHandler:
    async def connect_async(url, session):
        try:
            # Effettua una richiesta asincrona all'URL specificato
            async with session.get(url) as response:
                # Se la richiesta ha successo (status code 200)
                if response.status == 200:
                    # Legge i dati grezzi della risposta e li analizza con BeautifulSoup
                    raw_data = await response.read()
                    return BeautifulSoup(raw_data, 'html.parser')
                else:
                    # Restituisce None se la richiesta non ha avuto successo
                    return None
        except:
            # Restituisce None in caso di errore durante la richiesta
            return None


# SCRAPER DI "www.1000bit.it"
class Scraper1:
    url = "https://www.1000bit.it"

    # Ottiene i dati e le immagini di ogni dispositivo
    async def get_data(self, data):
        try:
            table = data.find('table')
        except:
            # Restituisce None in caso di mancanza della tabella dei dati
            return None

        all_images = data.find_all('img')
        image = {'img_link': None}
        device_images = []

        # Estrazione dei dati dalle immagini
        for i in all_images:
            img_link = i.get('src')
            if 'lista' in img_link:
                image['img_link'] = self.url + img_link
                caption = i.find_next('div').text.strip()
                if '\t' in caption:
                    image['caption'] = ''
                else:
                    image['caption'] = caption

                device_images.append(image)

        # Estrazione dei dati dalla tabella
        device = await ExtractionHandler.get_data_from_table(self, table)

        # Controllo sulla corretta estrazione dei dati
        try:
            device['Manufacturer']
        except:
            return None

        # Adattamento del dizionario a quello degli altri scraper
        device['MANUFACTURER'] = device.pop('Manufacturer')
        device['NAME'] = device.pop('Name')
        device['TYPE'] = device.pop('Type')
        device['ORIGIN'] = ''
        device['PRICE'] = device.pop('Original price')
        device['QUANTITY BUILT'] = device.pop('Units sold')
        device['I/OPORTS'] = device.pop('Others port')

        return {'DEVICE': device, 'IMAGES': device_images}

    async def scrape(self):
        # Logging per segnalare l'inizio dell'estrazione dal sito
        logging.info('Estrazione da www.1000bit.it')
        results = []

        # Utilizzo di aiohttp per gestire le richieste HTTP in modo asincrono
        async with aiohttp.ClientSession() as session:

            # Impostazione della lingua inglese per la navigazione sul sito
            await RequestHandler.connect_async(self.url + "/wrapper.asp?l=eng", session)

            # Crea una lista di task asincroni per ciascun link
            tasks = [RequestHandler.connect_async(self.url + f'/scheda.asp?id={x}', session) for x in range(3000)]

            # Esegue tutte le richieste in parallelo e attende i risultati
            responses = await asyncio.gather(*tasks)

        for res in responses:
            if res is not None:
                # Esegue l'estrazione dei dati e li aggiunge ai risultati
                result = await self.get_data(res)
                if result is not None:
                    results.append(result)

        # Logging per segnalare la fine dell'estrazione e il numero di risultati ottenuti
        logging.info(f'Estrazione terminata per www.1000bit.it - Risultati ottenuti: {len(results)}')

        # Chiama il metodo di inserimento dei dati nel database dalla classe ComputerDatabase
        ComputerDatabase.insertion(self, results)


# SCRAPER DI "www.vintage-computer.com/"
class Scraper2:
    url = 'https://www.vintage-computer.com/'

    # Ottiene i dati e le immagini di ogni dispositivo
    async def get_data(self, data):
        # Estrae la tabella contenente i dati del computer
        table = data.find('table')

        # Estrae tutte le immagini del computer, escludendo la prima
        all_images = data.find_all('img')
        all_images.pop(0)

        device_images = []
        for i in all_images:
            image = {}
            # Costruisce il link completo dell'immagine
            image['img_link'] = self.url + i.get('src')
            try:
                # Tenta di estrarre la didascalia dell'immagine
                image['caption'] = i.find_next('p').text
            except:
                image['caption'] = ''  # Se non ci sono didascalie, imposta una stringa vuota

            device_images.append(image)

        # Esegue l'estrazione dei dati dalla tabella
        device = await ExtractionHandler.get_data_from_table(self, table)

        # Adattamento del dizionario a quello degli altri scraper
        device.pop('Description')
        device['MANUFACTURER'] = device.pop('Manufacturer')
        device['NAME'] = device.pop('Model')
        device['TYPE'] = 'Home Computer'
        device['ORIGIN'] = device.pop('Country of Origin')
        device['PRICE'] = device.pop('Price')
        device['QUANTITY BUILT'] = device.pop('Number Produced')
        device['CPU'] = device.pop('Processor')
        device['SPEED'] = device.pop('Speed')
        device['I/OPORTS'] = device.pop('I/O')

        return {'DEVICE': device, 'IMAGES': device_images}

    async def get_links(self):
        async with aiohttp.ClientSession() as session:
            links = []
            data = await RequestHandler.connect_async(self.url, session)

            # Estrae tutti i link che contengono "machines"
            links = [self.url + a['href'] for a in data.find_all('a', href=True) if 'machines' in a['href']]

            return links

    async def scrape(self):
        # Logging per segnalare l'inizio dell'estrazione dal sito
        logging.info('Estrazione da www.vintage-computer.com')
        results = []

        # Ottiene tutti i link alle pagine dei dispositivi
        links = await self.get_links()

        async with aiohttp.ClientSession() as session:
            # Crea una lista di task asincroni per ciascun link
            tasks = [RequestHandler.connect_async(l, session) for l in links]

            # Esegue tutte le richieste in parallelo e attende i risultati
            responses = await asyncio.gather(*tasks)

            # Ricava le informazioni di ogni computer
            for res in responses:
                if res is not None:
                    results.append(await self.get_data(res))

        # Logging per segnalare la fine dell'estrazione e il numero di risultati ottenuti
        logging.info(f'Estrazione terminata per www.vintage-computer.com - Risultati ottenuti: {len(results)}')

        # Chiama il metodo di inserimento dei dati nel database dalla classe ComputerDatabase
        ComputerDatabase.insertion(self, results)


# SCRAPER DI "www.thepcmuseum.net"
class Scraper3:
    url = 'http://www.thepcmuseum.net/'

    # Ottiene i link alle pagine dei dispositivi
    async def getlinks(self):
        async with aiohttp.ClientSession() as session:
            links = []
            data = await RequestHandler.connect_async(Scraper3.url + '/model_results.php', session)
            all_links = data.find('table').find_all('a')
            for l in all_links:
                link = l.get('href')

                if 'details.php?' in link:
                    links.append(self.url + link.replace('(', '%28').replace(')', '%29'))

        return links

    # Ottiene i dati e le immagini di ogni dispositivo
    async def get_data(self, data):
        try:
            table = data.find('table').tbody
            images = data.find_all('img')
            image = {'img_link': None}

            # Estrae il link dell'immagine principale del computer
            for i in images:
                img_link = i.get('src')
                if 'comp_images' in img_link:
                    image['img_link'] = self.url + img_link

            try:
                # Tenta di estrarre la didascalia dell'immagine
                img_caption = data.find('p', class_='petitnoir').text
                image['caption'] = img_caption
            except:
                pass  # Se non c'è una didascalia disponibile, continua senza errori

            # Restituisce un dizionario contenente i dati del computer e le immagini
            return {'DEVICE': await ExtractionHandler.get_data_from_table(self, table), 'IMAGES': [image]}
        except:
            return None  # Se non sono presenti dati, restituisce None

    # Funzione principale per l'estrazione dei dati
    async def scrape(self):
        logging.info('Estrazione da www.thepcmuseum.net')
        results = []

        # Ottiene i link alle pagine di ogni dispositivo
        links = await self.getlinks()

        async with aiohttp.ClientSession() as session:
            # Crea una lista di task asincroni per ciascun link
            tasks = [RequestHandler.connect_async(l, session) for l in links]

            # Esegue tutte le richieste in parallelo e attende i risultati
            responses = await asyncio.gather(*tasks)

            # Ricava le informazioni di ogni dispositivo
            for res in responses:
                if res is not None:
                    # Esegue l'estrazione dei dati e li aggiunge ai risultati
                    result = await self.get_data(res)
                    if result is not None:
                        results.append(result)

        # Logging per segnalare la fine dell'estrazione e il numero di risultati ottenuti
        logging.info(f'Estrazione terminata per www.thepcmuseum.net - Risultati ottenuti: {len(results)}')

        # Chiama il metodo di inserimento dei dati nel database dalla classe ComputerDatabase
        ComputerDatabase.insertion(self, results)


# MAIN
if __name__ == '__main__':
    # IMPOSTAZIONI LOG
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Registra l'orario di inizio dell'estrazione
    initial_time = datetime.datetime.now()
    logging.info('Inizio estrazione')

    #crea le istanze di ogni scraper
    scraper_1, scraper_2, scraper_3 = Scraper1(), Scraper2(), Scraper3()

    #esegue gli scraper in modo asincrono
    loop = asyncio.get_event_loop()
    coroutines = [scraper_1.scrape(), scraper_2.scrape(), scraper_3.scrape()]
    loop.run_until_complete(asyncio.gather(*coroutines))

    # Registra l'orario di fine dell'estrazione
    final_time = datetime.datetime.now()
    logging.info('Fine estrazione')

    # Calcola e registra il tempo impiegato per l'estrazione
    logging.info(f'Tempo impiegato: {final_time - initial_time}')

