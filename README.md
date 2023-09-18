# ANAC Open Data
Scarica gli open data del portale ANAC e li carica in un database MySQL.

# Installare librerie

```pip install -r requirements.txt```

oppure

```conda install --file requirements.txt```

# Configurazione

Modificare il file ```STATEMENTS.py```:

Inserire nella variabile ```CREDENTIALS``` i dati corrispondenti per la connessione al database

# Definizione del DataBase

```python main.py load [OPTIONS]```

esegue il download e l'inserimento di tutti i file disponbili sul portale ANAC

**options:**

```-h --help``` restituisce informazioni sui comandi

```-s --skip``` permette di evitare il download dei file ed il caricamento per le tabelle indicate; default: "smartcig",
per cricare senza omissioni aggungere l'opzione senza argomenti

```-t --tables <NAME> ...``` permette di scaricare ed inserire i file relativi ad una o più tabelle

# Definizione dell tabella di sintesi

```python main.py sintesi```

esegue tutte le operazioni necessaria alla creazione e inserimento dei dati nella tabella "sintesi"


