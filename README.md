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

esegue il download e l'inserimento dei file disponbili sul portale ANAC

**options:**

```-h --help``` restituisce informazioni sui comandi

```-s --skip``` permette di evitare il download ed il caricamento dei file per le tabelle indicate; default: "smartcig",
Per scaricare senza omissioni aggiungere l'opzione senza argomenti.

```-t --tables <NAME> ...``` permette di scaricare ed inserire i file relativi ad una o più tabelle indicate


