__all__ = [
    'BATCH_SIZE', 'GET_TABLE_COLUMNS', 'INSERT_TABLES',
    'HASH_KEY', 'ADD_ID', 'INSERT_SINTESI', 'INSERT_LOADED',
    'GET_LOADED', 'RGX_DENOMINAZIONE', 'LAST_LOAD', 'CREATE_LOADED',
]


# CONSTANTS ################################################################################
# Set these values to configure

URL_ANAC = 'https://dati.anticorruzione.it/opendata/'

DB_CREDENTIALS = {'host': 'xxx.xxx.xxx.xxx',
                  'database': 'xxx',
                  'user': 'xxx',
                  'password': 'xxx'}

DEFAULT_DOWNLOAD_PATH = 'anac_json/'

BATCH_SIZE = 75_000


# SQL STATEMENTS ############################################################################

# GET_TABLE_COLUMNS = 'SHOW COLUMNS FROM {} WHERE extra = ""'

GET_TABLE_COLUMNS = '''
    SELECT
        COLUMN_NAME
    FROM
        INFORMATION_SCHEMA.COLUMNS
    WHERE
        TABLE_SCHEMA = DATABASE() AND
        TABLE_NAME = %s AND
        EXTRA = ""
    '''

GET_ALL_COLUMNS = '''
    SELECT
        TABLE_NAME,
        COLUMN_NAME
    FROM
        INFORMATION_SCHEMA.COLUMNS
    WHERE
        TABLE_SCHEMA = DATABASE() AND
        EXTRA = ""
    '''

INSERT_TABLES = 'INSERT IGNORE INTO {} ({}) VALUES({})'

HASH_KEY = '''ALTER TABLE {} ADD COLUMN {}_hash BINARY(20) AS
(UNHEX(SHA(CONCAT_WS(";",{})))) STORED INVISIBLE UNIQUE'''

ADD_ID = 'ALTER TABLE {} ADD COLUMN {}_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY'

CREATE_LOADED = '''
    CREATE TABLE IF NOT EXISTS loaded (
        -- loaded_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        table_name VARCHAR(64) NOT NULL,
        file_name VARCHAR(128) NOT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        -- KEY idx_loaded_table_name (table_name),
        PRIMARY KEY id_loaded_file (table_name, file_name))'''


GET_LOADED = 'SELECT file_name FROM loaded'

INSERT_LOADED = 'INSERT IGNORE INTO loaded (table_name, file_name) VALUES(%s, %s)'

CREATE_TABLES = {
    'aggiudicatari': '''
    CREATE TABLE IF NOT EXISTS aggiudicatari (
        cig VARCHAR(64) DEFAULT NULL,
        ruolo VARCHAR(64) DEFAULT NULL,
        codice_fiscale VARCHAR(64) DEFAULT NULL,
        denominazione VARCHAR(384) DEFAULT NULL,
        tipo_soggetto VARCHAR(384) DEFAULT NULL,
        id_aggiudicazione BIGINT UNSIGNED DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_aggiudicatari_id_aggiudicazione (id_aggiudicazione),
        KEY idx_aggiudicatari_cig (cig))''',

    'aggiudicazioni': '''
    CREATE TABLE IF NOT EXISTS aggiudicazioni (
        cig VARCHAR(64) DEFAULT NULL,
        data_aggiudicazione_definitiva DATETIME DEFAULT NULL,
        esito VARCHAR(384) DEFAULT NULL,
        criterio_aggiudicazione VARCHAR(384) DEFAULT NULL,
        data_comunicazione_esito DATETIME DEFAULT NULL,
        numero_offerte_ammesse INT DEFAULT NULL,
        numero_offerte_escluse INT DEFAULT NULL,
        importo_aggiudicazione DOUBLE DEFAULT NULL,
        ribasso_aggiudicazione DOUBLE DEFAULT NULL,
        num_imprese_offerenti INT DEFAULT NULL,
        flag_subappalto TINYINT UNSIGNED DEFAULT NULL,
        id_aggiudicazione BIGINT UNSIGNED DEFAULT NULL,
        cod_esito TINYINT UNSIGNED DEFAULT NULL,
        num_imprese_richiedenti INT DEFAULT NULL,
        asta_elettronica TINYINT UNSIGNED DEFAULT NULL,
        num_imprese_invitate INT DEFAULT NULL,
        massimo_ribasso FLOAT DEFAULT NULL,
        minimo_ribasso FLOAT DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_aggiudicazioni_id_aggiudicazione (id_aggiudicazione),
        KEY idx_aggiudicazioni_cig (cig))''',

    'attestazioni_soa': '''
    CREATE TABLE IF NOT EXISTS attestazioni_soa (
        cf_soa VARCHAR(64) DEFAULT NULL,
        denom_soa VARCHAR(384) DEFAULT NULL,
        num_protocollo_autorizzazione INT DEFAULT NULL,
        data_autorizzazione DATETIME DEFAULT NULL,
        num_attestazione VARCHAR(64) DEFAULT NULL,
        regolamento VARCHAR(384) DEFAULT NULL,
        data_emissione DATETIME DEFAULT NULL,
        anno_emissione SMALLINT DEFAULT NULL,
        data_rilascio_originaria DATETIME DEFAULT NULL,
        data_scadenza_verifica DATETIME DEFAULT NULL,
        data_scadenza_finale DATETIME DEFAULT NULL,
        fase_attestato VARCHAR(64) DEFAULT NULL,
        alla_data_del DATETIME DEFAULT NULL,
        cf_impresa VARCHAR(64) DEFAULT NULL,
        denom_impresa VARCHAR(384) DEFAULT NULL,
        numAttPrecedente VARCHAR(64) DEFAULT NULL,
        enteRilcertQualita VARCHAR(384) DEFAULT NULL,
        certificazioneDiQualitaScadenza DATETIME DEFAULT NULL,
        data_effettuazione_verifica DATETIME DEFAULT NULL,
        cod_categoria SMALLINT DEFAULT NULL,
        categoria VARCHAR(64) DEFAULT NULL,
        desc_categoria VARCHAR(384) DEFAULT NULL,
        classifica VARCHAR(64) DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_attestazioni_soa_cf_impresa (cf_impresa))''',

    'avvio_contratto': '''
    CREATE TABLE IF NOT EXISTS avvio_contratto (
        cig VARCHAR(64) DEFAULT NULL,
        data_stipula_contratto DATETIME DEFAULT NULL,
        data_esecutivita_contratto DATETIME DEFAULT NULL,
        data_termine_contrattuale DATETIME DEFAULT NULL,
        data_verbale_consegna_definitiva DATETIME DEFAULT NULL,
        data_inizio_effettiva DATETIME DEFAULT NULL,
        data_verbale_prima_consegna DATETIME DEFAULT NULL,
        consegna_frazionata TINYINT UNSIGNED DEFAULT NULL,
        consegna_sotto_riserva TINYINT UNSIGNED DEFAULT NULL,
        id_aggiudicazione BIGINT UNSIGNED DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_avvio_contratto_cig (cig),
        KEY idx_avvio_contratto_id_aggiudicazione (id_aggiudicazione))''',

    'cig': '''
    CREATE TABLE IF NOT EXISTS cig (
        cig VARCHAR(64) DEFAULT NULL,
        cig_accordo_quadro VARCHAR(64) DEFAULT NULL,
        numero_gara BIGINT DEFAULT NULL,
        oggetto_gara TEXT DEFAULT NULL,
        importo_complessivo_gara DOUBLE DEFAULT NULL,
        n_lotti_componenti MEDIUMINT DEFAULT NULL,
        oggetto_lotto TEXT DEFAULT NULL,
        importo_lotto DOUBLE DEFAULT NULL,
        oggetto_principale_contratto VARCHAR(64) DEFAULT NULL,
        stato VARCHAR(64) DEFAULT NULL,
        settore VARCHAR(64) DEFAULT NULL,
        luogo_istat VARCHAR(64) DEFAULT NULL,
        provincia VARCHAR(64) DEFAULT NULL,
        data_pubblicazione DATETIME DEFAULT NULL,
        data_scadenza_offerta DATETIME DEFAULT NULL,
        cod_tipo_scelta_contraente INT UNSIGNED DEFAULT NULL,
        tipo_scelta_contraente VARCHAR(384) DEFAULT NULL,
        cod_modalita_realizzazione SMALLINT DEFAULT NULL,
        modalita_realizzazione VARCHAR(384) DEFAULT NULL,
        codice_ausa VARCHAR(64) DEFAULT NULL,
        cf_amministrazione_appaltante VARCHAR(64) DEFAULT NULL,
        denominazione_amministrazione_appaltante VARCHAR(384) DEFAULT NULL,
        sezione_regionale VARCHAR(64) DEFAULT NULL,
        id_centro_costo VARCHAR(64) DEFAULT NULL,
        denominazione_centro_costo VARCHAR(384) DEFAULT NULL,
        anno_pubblicazione SMALLINT DEFAULT NULL,
        mese_pubblicazione TINYINT UNSIGNED DEFAULT NULL,
        cod_cpv VARCHAR(64) DEFAULT NULL,
        descrizione_cpv VARCHAR(384) DEFAULT NULL,
        flag_prevalente TINYINT UNSIGNED DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_cig_cig (cig),
        KEY idx_cig_cod_cpv (cod_cpv),
        KEY idx_cig_centro_costo (id_centro_costo),
        KEY idx_cig_codice_ausa (codice_ausa),
        KEY idx_cig_numero_gara (numero_gara),
        KEY idx_cig_cig_numero_gara (cig,numero_gara),
        KEY idx_cig_cf_amministrazione_appaltante (cf_amministrazione_appaltante))''',

    'smartcig': '''
    CREATE TABLE IF NOT EXISTS smartcig (
        cig VARCHAR(64) DEFAULT NULL,
        numero_gara VARCHAR(64) DEFAULT NULL,
        oggetto_gara TEXT DEFAULT NULL,
        importo_complessivo_gara DOUBLE DEFAULT NULL,
        n_lotti_componenti MEDIUMINT DEFAULT NULL,
        oggetto_lotto TEXT DEFAULT NULL,
        importo_lotto DOUBLE DEFAULT NULL,
        oggetto_principale_contratto VARCHAR(64) DEFAULT NULL,
        stato VARCHAR(64) DEFAULT NULL,
        data_comunicazione DATETIME DEFAULT NULL,
        id_tipo_fattispecie_contrattuale VARCHAR(64) DEFAULT NULL,
        tipo_fattispecie_contrattuale VARCHAR(384) DEFAULT NULL,
        cod_tipo_scelta_contraente SMALLINT DEFAULT NULL,
        tipo_scelta_contraente VARCHAR(384) DEFAULT NULL,
        codice_ausa VARCHAR(64) DEFAULT NULL,
        cf_amministrazione_appaltante VARCHAR(64) DEFAULT NULL,
        denominazione_amministrazione_appaltante VARCHAR(384) DEFAULT NULL,
        sezione_regionale VARCHAR(64) DEFAULT NULL,
        indirizzo VARCHAR(384) DEFAULT NULL,
        istat_comune VARCHAR(64) DEFAULT NULL,
        citta VARCHAR(64) DEFAULT NULL,
        regione VARCHAR(64) DEFAULT NULL,
        id_centro_costo VARCHAR(64) DEFAULT NULL,
        denominazione_centro_costo VARCHAR(384) DEFAULT NULL,
        anno_comunicazione SMALLINT DEFAULT NULL,
        mese_comunicazione TINYINT UNSIGNED DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_smartcig_cig (cig))''',

    'categorie_dpcm_aggregazione': '''
    CREATE TABLE IF NOT EXISTS categorie_dpcm_aggregazione (
        cig VARCHAR(32) DEFAULT NULL,
        cod_categoria_merceologica_dpcm_aggregazione SMALLINT DEFAULT NULL,
        categoria_merceologica_dpcm_aggregazione VARCHAR(384) DEFAULT NULL,
        cod_deroga_soggetto_aggregatore TINYINT UNSIGNED DEFAULT NULL,
        deroga_dpcm_soggetto_aggregatore VARCHAR(384) DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_categorie_dpcm_aggregazione_cig (cig))''',

    'categorie_opera': '''
    CREATE TABLE IF NOT EXISTS categorie_opera (
        cig VARCHAR(64) DEFAULT NULL,
        id_categoria VARCHAR(64) DEFAULT NULL,
        descrizione VARCHAR(384) DEFAULT NULL,
        cod_tipo_categoria VARCHAR(64) DEFAULT NULL,
        descrizione_tipo_categoria VARCHAR(64) DEFAULT NULL,
        classe_importo VARCHAR(64) DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_categorie_opera_cig (cig))''',

    'centri_di_costo': '''
    CREATE TABLE IF NOT EXISTS centri_di_costo (
        id_centro_di_costo VARCHAR(64) DEFAULT NULL,
        denominazione_centro_di_costo VARCHAR(384) DEFAULT NULL,
        codice_ausa VARCHAR(64) DEFAULT NULL,
        stato VARCHAR(64) DEFAULT NULL,
        data_inizio DATETIME DEFAULT NULL,
        data_fine DATETIME DEFAULT NULL,
        provincia_codice VARCHAR(64) DEFAULT NULL,
        provincia_nome VARCHAR(64) DEFAULT NULL,
        citta_codice VARCHAR(64) DEFAULT NULL,
        citta_nome VARCHAR(64) DEFAULT NULL,
        indirizzo VARCHAR(384) DEFAULT NULL,
        cap VARCHAR(64) DEFAULT NULL,
        flag_soggetto_aggregatore TINYINT UNSIGNED DEFAULT NULL,
        stazione_appaltante_codice_fiscale VARCHAR(64) DEFAULT NULL,
        stazione_appaltante_denominazione VARCHAR(384) DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_centri_di_costo_id_centro_di_costo (id_centro_di_costo))''',

    'collaudo': '''
    CREATE TABLE IF NOT EXISTS collaudo (
        cig VARCHAR(64) DEFAULT NULL,
        data_delibera DATETIME DEFAULT NULL,
        data_cert_collaudo DATETIME DEFAULT NULL,
        esito_collaudo VARCHAR(64) DEFAULT NULL,
        data_inizio_oper DATETIME DEFAULT NULL,
        data_regolare_esec DATETIME DEFAULT NULL,
        data_nomina_coll DATETIME DEFAULT NULL,
        data_collaudo_stat DATETIME DEFAULT NULL,
        id_aggiudicazione BIGINT UNSIGNED DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_collaudo_cig (cig),
        KEY idx_collaudo_id_aggiudicazione (id_aggiudicazione))''',

    'cpv': '''
    CREATE TABLE IF NOT EXISTS cpv (
        cod_cpv_ VARCHAR(64) DEFAULT NULL,
        descrizione_cpv_ VARCHAR(384) DEFAULT NULL,
        divisione INT UNSIGNED DEFAULT NULL,
        gruppo INT UNSIGNED DEFAULT NULL,
        classe INT UNSIGNED DEFAULT NULL,
        categoria INT UNSIGNED DEFAULT NULL,
        sub_categ INT UNSIGNED DEFAULT NULL,
        sub_sub_categ INT UNSIGNED DEFAULT NULL,
        sub_sub_sub_categ INT UNSIGNED DEFAULT NULL,
        IT_descrizione_divisione VARCHAR(384) DEFAULT NULL,
        IT_descrizione_gruppo VARCHAR(384) DEFAULT NULL,
        IT_descrizione_classe VARCHAR(384) DEFAULT NULL,
        IT_descrizione_categorie VARCHAR(384) DEFAULT NULL,
        IT_descrizione_sub_categorie VARCHAR(384) DEFAULT NULL,
        IT_descrizione_sub_sub_categorie VARCHAR(384) DEFAULT NULL,
        IT_descrizione_sub_sub_sub_categorie VARCHAR(384) DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_cpv_cod_cpv_ (cod_cpv_),
        KEY idx_cpv_IT_descrizione_divisione (IT_descrizione_divisione),
        KEY idx_cpv_IT_descrizione_gruppo (IT_descrizione_gruppo),
        KEY idx_cpv_IT_descrizione_classe (IT_descrizione_classe),
        KEY idx_cpv_IT_descrizione_categorie (IT_descrizione_categorie),
        KEY idx_cpv_IT_descrizione_sub_categorie (IT_descrizione_sub_categorie),
        KEY idx_cpv_IT_descrizione_sub_sub_categorie (IT_descrizione_sub_sub_categorie),
        KEY idx_cpv_IT_descrizione_sub_sub_sub_categorie (IT_descrizione_sub_sub_sub_categorie))''',

    'cup': '''
    CREATE TABLE IF NOT EXISTS cup (
        cig VARCHAR(64) DEFAULT NULL,
        cup VARCHAR(64) DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_cup_cig (cig))''',

    'empulia': '''
    CREATE TABLE IF NOT EXISTS empulia (
        descrizione_breve TEXT DEFAULT NULL,
        cig VARCHAR(64) DEFAULT NULL,
        descrizione_proponente VARCHAR(384) DEFAULT NULL,
        incaricato VARCHAR(64) DEFAULT NULL,
        importo_appalto DOUBLE DEFAULT NULL,
        importo_base_asta DOUBLE DEFAULT NULL,
        criterio_aggiudicazione VARCHAR(64) DEFAULT NULL,
        tipo_appalto VARCHAR(64) DEFAULT NULL,
        termine_richiesta_quesiti VARCHAR(384) DEFAULT NULL,
        termine_richiesta_presentazione_offerte VARCHAR(384) DEFAULT NULL,
        data_seduta DATETIME DEFAULT NULL,
        note TEXT,
        list_of_file json DEFAULT NULL,
        expired TINYINT UNSIGNED DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_empulia_cig (cig))''',

    'fine_contratto': '''
    CREATE TABLE IF NOT EXISTS fine_contratto (
        cig VARCHAR(64) DEFAULT NULL,
        cod_motivo_risoluzione TINYINT UNSIGNED DEFAULT NULL,
        motivo_risoluzione VARCHAR(384) DEFAULT NULL,
        cod_motivo_interruzione_anticipata TINYINT UNSIGNED DEFAULT NULL,
        motivo_interruzione_anticipata VARCHAR(384) DEFAULT NULL,
        data_conclusione_anticipata DATETIME DEFAULT NULL,
        data_effettiva_ultimazione DATETIME DEFAULT NULL,
        id_aggiudicazione BIGINT UNSIGNED DEFAULT NULL,
        giorni_proroga SMALLINT DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_fine_contratto_cig (cig),
        KEY idx_fine_contratto_id_aggiudicazione (id_aggiudicazione))''',

    'fonti_finanziamento': '''
    CREATE TABLE IF NOT EXISTS fonti_finanziamento (
        cig VARCHAR(64) DEFAULT NULL,
        entrate_con_dest_vincolata_pubblica_nazionale_regionale DOUBLE DEFAULT NULL,
        fondi_di_bilancio_dellamministrazione_competente DOUBLE DEFAULT NULL,
        mutuo DOUBLE DEFAULT NULL,
        entrate_con_dest_vincolata_pubblica_nazionale_centrale DOUBLE DEFAULT NULL,
        id_aggiudicazione BIGINT UNSIGNED DEFAULT NULL,
        entrate_con_dest_vincolata_pubblica_nazionale_locale DOUBLE DEFAULT NULL,
        altro DOUBLE DEFAULT NULL,
        entrate_con_dest_vincolata_pubblica_nazionale_altri DOUBLE DEFAULT NULL,
        apporto_di_capitali_privati DOUBLE DEFAULT NULL,
        sfruttamento_economico_e_funzionale_del_bene DOUBLE DEFAULT NULL,
        fondi_di_bilancio_della_stazione_appaltante DOUBLE DEFAULT NULL,
        entrate_con_dest_vincolata_pubblica_comunitaria DOUBLE DEFAULT NULL,
        entrate_con_dest_vincolata_privati DOUBLE DEFAULT NULL,
        trasferimento_di_immobili_ex_art53_c6_dlgs_n163_06 DOUBLE DEFAULT NULL,
        #trasferimento_di_immobili_ex_art53_c6_dlgs_n163_06_economia_su_stanziamenti_non_vincolati DOUBLE DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_fonti_finanziamento_cig (cig),
        KEY idx_fonti_finanziamento_id_aggiudicazione (id_aggiudicazione))''',

    'lavorazioni': '''
    CREATE TABLE IF NOT EXISTS lavorazioni (
        cig VARCHAR(64) DEFAULT NULL,
        cod_tipo_lavorazione TINYINT UNSIGNED DEFAULT NULL,
        tipo_lavorazione VARCHAR(64) DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_lavorazioni_cig (cig))''',

    'bandi_cig_modalita_realizzazione':
    '''CREATE TABLE IF NOT EXISTS bandi_cig_modalita_realizzazione (
        modalita_realizzazione_codice INT DEFAULT NULL,
        modalita_realizzazione_denominazione VARCHAR(384) DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ))''',

    'province': '''
    CREATE TABLE IF NOT EXISTS province (
        Sigla VARCHAR(64) DEFAULT NULL,
        provincia VARCHAR(64) DEFAULT NULL,
        regione VARCHAR(64) DEFAULT NULL,
        provincia_codice VARCHAR(64) DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ))''',

    'pubblicazioni': '''
    CREATE TABLE IF NOT EXISTS pubblicazioni (
        cig VARCHAR(64) DEFAULT NULL,
        data_creazione DATETIME DEFAULT NULL,
        data_albo DATETIME DEFAULT NULL,
        data_guri DATETIME DEFAULT NULL,
        data_guce DATETIME DEFAULT NULL,
        data_bore DATETIME DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_pubblicazioni_cig (cig))''',

    'quadro_economico': '''
    CREATE TABLE IF NOT EXISTS quadro_economico (
        cig VARCHAR(64) DEFAULT NULL,
        data DATETIME DEFAULT NULL,
        descrizione_evento VARCHAR(64) DEFAULT NULL,
        importo_sicurezza DOUBLE DEFAULT NULL,
        dettaglio_evento INT DEFAULT NULL,
        importo_forniture DOUBLE DEFAULT NULL,
        importo_lavori DOUBLE DEFAULT NULL,
        importo_progettazione DOUBLE DEFAULT NULL,
        id_aggiudicazione BIGINT UNSIGNED DEFAULT NULL,
        somme_a_disposizione DOUBLE DEFAULT NULL,
        importo_servizi DOUBLE DEFAULT NULL,
        ulteriori_oneri_non_soggetti_ribasso DOUBLE DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_quadro_economico_cig (cig),
        KEY idx_quadro_economico_id_aggiudicazione (id_aggiudicazione))''',

    'sospensioni': '''
    CREATE TABLE IF NOT EXISTS sospensioni (
        cig VARCHAR(64) DEFAULT NULL,
        data_sospensione DATETIME DEFAULT NULL,
        data_ripresa DATETIME DEFAULT NULL,
        descrizione_motivo VARCHAR(384) DEFAULT NULL,
        id_aggiudicazione BIGINT UNSIGNED DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_sospensioni_cig (cig),
        KEY idx_sospensioni_id_aggiudicazione (id_aggiudicazione))''',

    'stati_avanzamento': '''
    CREATE TABLE IF NOT EXISTS stati_avanzamento (
        cig VARCHAR(64) DEFAULT NULL,
        denominazione_sal TEXT DEFAULT NULL,
        flag_ritardo VARCHAR(64) DEFAULT NULL,
        data_emissione_sal DATETIME DEFAULT NULL,
        importo_sal DOUBLE DEFAULT NULL,
        n_giorni_scostamento INT DEFAULT NULL,
        progressivo_sal INT DEFAULT NULL,
        id_aggiudicazione BIGINT UNSIGNED DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_stati_di_avanzamento_cig (cig),
        KEY idx_stati_di_avanzamento_id_aggiudicazione (id_aggiudicazione))''',

    'stazioni_appaltanti': '''
    CREATE TABLE IF NOT EXISTS stazioni_appaltanti (
        codice_fiscale VARCHAR(64) DEFAULT NULL,
        partita_iva VARCHAR(64) DEFAULT NULL,
        denominazione VARCHAR(384) DEFAULT NULL,
        codice_ausa VARCHAR(64) DEFAULT NULL,
        natura_giuridica_codice VARCHAR(64) DEFAULT NULL,
        natura_giuridica_descrizione VARCHAR(384) DEFAULT NULL,
        soggetto_estero TINYINT UNSIGNED DEFAULT NULL,
        provincia_codice VARCHAR(64) DEFAULT NULL,
        provincia_nome VARCHAR(64) DEFAULT NULL,
        citta_codice VARCHAR(64) DEFAULT NULL,
        citta_nome VARCHAR(64) DEFAULT NULL,
        indirizzo_odonimo VARCHAR(384) DEFAULT NULL,
        cap VARCHAR(64) DEFAULT NULL,
        flag_inHouse TINYINT UNSIGNED DEFAULT NULL,
        flag_partecipata TINYINT UNSIGNED DEFAULT NULL,
        stato VARCHAR(64) DEFAULT NULL,
        data_inizio DATETIME DEFAULT NULL,
        data_fine DATETIME DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_stazione_appaltante_codice_fiscale (codice_fiscale),
        KEY idx_stazione_appaltante_codice_ausa (codice_ausa),
        KEY idx_stazione_appaltante_provincia_nome (provincia_nome))''',

    'subappalti': '''
    CREATE TABLE IF NOT EXISTS subappalti (
        id_subappalto VARCHAR(64) DEFAULT NULL,
        cig VARCHAR(64) DEFAULT NULL,
        cf_subappaltante VARCHAR(64) DEFAULT NULL,
        data_autorizzazione DATETIME DEFAULT NULL,
        oggetto TEXT DEFAULT NULL,
        ruolo VARCHAR(384) DEFAULT NULL,
        codice_fiscale VARCHAR(64) DEFAULT NULL,
        denominazione VARCHAR(384) DEFAULT NULL,
        tipo_soggetto VARCHAR(384) DEFAULT NULL,
        descrizione_categoria VARCHAR(384) DEFAULT NULL,
        classe_importo VARCHAR(384) DEFAULT NULL,
        cod_categoria VARCHAR(64) DEFAULT NULL,
        descrizione_tipo_categoria VARCHAR(384) DEFAULT NULL,
        cod_cpv VARCHAR(64) DEFAULT NULL,
        descrizione_cpv VARCHAR(384) DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_subappalti_cig (cig),
        KEY idx_subappalti_cf_subappaltante (cf_subappaltante),
        KEY idx_subappalti_codice_fiscale (codice_fiscale))''',

    'tipo_fattispecie_contrattuale': '''
    CREATE TABLE IF NOT EXISTS tipo_fattispecie_contrattuale (
        tipo_fattispecie_contrattuale_id VARCHAR(64) DEFAULT NULL,
        tipo_fattispecie_contrattuale_denominazione VARCHAR(384) DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ))''',

    'bandi_cig_tipo_scelta_contraente': '''
    CREATE TABLE IF NOT EXISTS bandi_cig_tipo_scelta_contraente (
        tipo_scelta_contraente_codice INT DEFAULT NULL,
        tipo_scelta_contraente_denominazione VARCHAR(384) DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ))''',

    'varianti': '''
    CREATE TABLE IF NOT EXISTS varianti (
        id_variante BIGINT DEFAULT NULL,
        cod_motivo_variante INT DEFAULT NULL,
        motivo_variante VARCHAR(384) DEFAULT NULL,
        data_approvazione_variante DATETIME DEFAULT NULL,
        cig VARCHAR(64) DEFAULT NULL,
        id_aggiudicazione BIGINT UNSIGNED DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_varianti_cig (cig),
        KEY idx_varianti_id_aggiudicazione (id_aggiudicazione))''',

    'partecipanti': '''
    CREATE TABLE IF NOT EXISTS partecipanti (
        cig VARCHAR(64) DEFAULT NULL,
        ruolo VARCHAR(64) DEFAULT NULL,
        codice_fiscale VARCHAR(64) DEFAULT NULL,
        denominazione VARCHAR(384) DEFAULT NULL,
        tipo_soggetto VARCHAR(384) DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_partecipanti (cig))''',

    'smartcig_tipo_fattispecie_contrattuale': '''
    CREATE TABLE IF NOT EXISTS smartcig_tipo_fattispecie_contrattuale (
        tipo_fattispecie_contrattuale_id VARCHAR(64) DEFAULT NULL,
        tipo_fattispecie_contrattuale_denominazione VARCHAR(384) DEFAULT NULL,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP )) ''',
}

TABLES = tuple(CREATE_TABLES)

RGX_DENOMINAZIONE = r"\\.|\\?|\\'|\\|&#\\d+;|&\\w+;|^&|^,|,$|^\\.|\\*"

CREATE_SINTESI = {
    'sintesi': '''
    CREATE TABLE IF NOT EXISTS sintesi (
        codice_fiscale VARCHAR(32) DEFAULT NULL,
        denominazione VARCHAR(384) DEFAULT NULL,
        ruolo VARCHAR(32) DEFAULT NULL,
        tipo_soggetto VARCHAR(256) DEFAULT NULL,
        importo_aggiudicazione DOUBLE DEFAULT NULL,
        importo_lotto DOUBLE DEFAULT NULL,
        data_aggiudicazione_definitiva DATETIME DEFAULT NULL,
        criterio_aggiudicazione VARCHAR(128) DEFAULT NULL,
        massimo_ribasso FLOAT DEFAULT NULL,
        minimo_ribasso FLOAT DEFAULT NULL,
        numero_offerte_ammesse INT DEFAULT NULL,
        numero_offerte_escluse INT DEFAULT NULL,
        num_imprese_offerenti INT DEFAULT NULL,
        cig VARCHAR(32) DEFAULT NULL,
        cod_cpv VARCHAR(32) DEFAULT NULL,
        descrizione_cpv VARCHAR(384) DEFAULT NULL,
        oggetto_lotto TEXT DEFAULT NULL,
        oggetto_gara TEXT DEFAULT NULL,
        tipo_scelta_contraente VARCHAR(128) DEFAULT NULL,
        oggetto_principale_contratto VARCHAR(16) DEFAULT NULL,
        modalita_realizzazione VARCHAR(128) DEFAULT NULL,
        regione VARCHAR(32) DEFAULT NULL,
        provincia VARCHAR(64) DEFAULT NULL,
        stazione_appaltante VARCHAR(256) DEFAULT NULL,
        cf_stazione_appaltante VARCHAR(32) DEFAULT NULL,
        provincia_codice VARCHAR(8) DEFAULT NULL,
        numero_gara BIGINT UNSIGNED DEFAULT NULL,
        anno INT UNSIGNED AS (YEAR(data_aggiudicazione_definitiva)) STORED,
        flag_subappalto TINYINT UNSIGNED DEFAULT NULL,
        flag_cons TINYINT UNSIGNED AS (CASE
            WHEN tipo_soggetto LIKE 'consorzio%' THEN 1
            WHEN tipo_soggetto LIKE 'ati%'THEN 1
            ELSE 0
            END) STORED,
        data_inserimento DATETIME DEFAULT (CURRENT_TIMESTAMP ),
        KEY idx_sintesi_data_inserimento (data_inserimento),
        KEY idx_sintesi_cod_cpv_provincia_anno (cod_cpv,provincia,anno),
        KEY idx_sintesi_regione_codice_fiscale (regione,codice_fiscale),
        KEY idx_sintesi_provincia_codice_fiscale (provincia,codice_fiscale),
        KEY idx_sintesi_denominazione_codice_fiscale (denominazione,codice_fiscale),
        KEY idx_sintesi_stazione_appaltante_codice_fiscale (stazione_appaltante,codice_fiscale),
        KEY idx_sintesi_descrizione_cpv_codice_fiscale (descrizione_cpv,codice_fiscale),
        KEY idx_sintesi_ruolo_codice_fiscale (ruolo,codice_fiscale),
        KEY idx_sintesi_codice_fiscale (codice_fiscale),
        KEY idx_sintesi_codFisc_descrizioneCPV (codice_fiscale,descrizione_cpv),
        KEY idx_sintesi_regione_descrizioneCPV (regione,descrizione_cpv),
        KEY idx_sintesi_data_aggiudicazione_definitiva (data_aggiudicazione_definitiva),
        KEY idx_sintesi_data_aggiudicazione_definitiva_denominazione (data_aggiudicazione_definitiva,denominazione),
        KEY idx_sintesi_codice_fiscale_criterio_aggiudicazione (codice_fiscale,criterio_aggiudicazione))'''
}

LAST_LOAD = 'SELECT MAX(data_inserimento) AS last_ins FROM sintesi'

INSERT_SINTESI = '''
    INSERT IGNORE INTO sintesi (codice_fiscale,
        denominazione,
        ruolo,
        tipo_soggetto,
        importo_aggiudicazione,
        importo_lotto,
        data_aggiudicazione_definitiva,
        criterio_aggiudicazione,
        massimo_ribasso,
        minimo_ribasso,
        numero_offerte_ammesse,
        numero_offerte_escluse,
        num_imprese_offerenti,
        cig,
        cod_cpv,
        descrizione_cpv,
        oggetto_lotto,
        oggetto_gara,
        tipo_scelta_contraente,
        oggetto_principale_contratto,
        modalita_realizzazione,
        regione,
        provincia,
        stazione_appaltante,
        cf_stazione_appaltante,
        provincia_codice,
        numero_gara,
        flag_subappalto)
    WITH
        ag AS (
            SELECT
                codice_fiscale,
                denominazione,
                ruolo,
                tipo_soggetto,
                id_aggiudicazione
            FROM aggiudicatari
            WHERE data_inserimento > %s),
        a AS (
            SELECT
                cig,
                importo_aggiudicazione,
                data_aggiudicazione_definitiva,
                criterio_aggiudicazione,
                massimo_ribasso,
                minimo_ribasso,
                numero_offerte_ammesse,
                numero_offerte_escluse,
                num_imprese_offerenti,
                flag_subappalto,
                id_aggiudicazione
            FROM aggiudicazioni
            WHERE data_inserimento > %s),
        c AS (
            SELECT
                importo_lotto,
                cig,
                cod_cpv,
                descrizione_cpv,
                oggetto_lotto,
                oggetto_gara,
                tipo_scelta_contraente,
                oggetto_principale_contratto,
                modalita_realizzazione,
                numero_gara,
                cf_amministrazione_appaltante
            FROM cig
            WHERE data_inserimento > %s)
    SELECT
        ag.codice_fiscale,
        TRIM(regexp_replace(ag.denominazione, %s, "")) AS denominazione,
        ag.ruolo,
        ag.tipo_soggetto,
        a.importo_aggiudicazione,
        c.importo_lotto,
        a.data_aggiudicazione_definitiva,
        a.criterio_aggiudicazione,
        a.massimo_ribasso,
        a.minimo_ribasso,
        a.numero_offerte_ammesse,
        a.numero_offerte_escluse,
        a.num_imprese_offerenti,
        c.cig,
        c.cod_cpv,
        TRIM(TRAILING '.' FROM c.descrizione_cpv) AS descrizione_cpv,
        c.oggetto_lotto,
        c.oggetto_gara,
        c.tipo_scelta_contraente,
        c.oggetto_principale_contratto,
        c.modalita_realizzazione,
        pr.regione,
        pr.provincia,
        sa.denominazione AS stazione_appaltante,
        sa.codice_fiscale AS cf_stazione_appaltante,
        sa.provincia_codice,
        c.numero_gara,
        a.flag_subappalto
    FROM
        ag
    JOIN
        a ON a.id_aggiudicazione = ag.id_aggiudicazione
    JOIN
        c ON c.cig = a.cig
    JOIN
        stazioni_appaltanti sa ON sa.codice_fiscale = c.cf_amministrazione_appaltante
    JOIN
    province pr ON sa.provincia_codice = pr.provincia_codice
    '''

CREATEVIEW_SINTESI_CPV = {
    'sintesi_cpv': '''
    CREATE OR REPLACE VIEW siap_test.sintesi_cpv AS
        SELECT
            s.codice_fiscale,
            s.denominazione,
            s.ruolo,
            s.tipo_soggetto,
            s.importo_aggiudicazione,
            s.importo_lotto,
            s.data_aggiudicazione_definitiva,
            s.criterio_aggiudicazione,
            s.massimo_ribasso,
            s.minimo_ribasso,
            s.numero_offerte_ammesse,
            s.numero_offerte_escluse,
            s.num_imprese_offerenti,
            s.cig,
            s.cod_cpv,
            s.descrizione_cpv,
            s.oggetto_lotto,
            s.oggetto_gara,
            s.tipo_scelta_contraente,
            s.oggetto_principale_contratto,
            s.modalita_realizzazione,
            s.regione,
            s.provincia,
            s.stazione_appaltante,
            s.cf_stazione_appaltante,
            s.provincia_codice,
            s.numero_gara,
            s.anno,
            s.flag_subappalto,
            s.flag_cons,
            c.divisione,
            c.gruppo,
            c.classe,
            c.categoria,
            c.sub_categ,
            c.sub_sub_categ,
            c.sub_sub_sub_categ,
            c.IT_descrizione_divisione,
            c.IT_descrizione_gruppo,
            c.IT_descrizione_classe,
            c.IT_descrizione_categorie,
            c.IT_descrizione_sub_categorie,
            c.IT_descrizione_sub_sub_categorie,
            c.IT_descrizione_sub_sub_sub_categorie
        FROM
            sintesi s
        JOIN cpv c
            ON s.cod_cpv = c.cod_cpv_
        WITH CHECK OPTION'''
}
