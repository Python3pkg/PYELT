from pyelt.mappings.sor_to_dv_mappings import SorToRefMapping
from pyelt.pipeline import Pipeline
from pyelt.helpers.encrypt import SimpleEncrypt

general_config = {
    'log_path': '/logs/',
    'ddl_log_path': '/logs/ddl/',
    'sql_log_path': '/logs/sql/',
    'conn_dwh': 'postgresql://postgres:Welkomnlhc2016@localhost:5432/dwh2',
    'debug': False,
    'ask_confirm_on_db_changes': False,
    'on_errors': 'log',
    'datatransfer_path': 'c:/tmp/',
    'data_root': 'C:/!OntwikkelDATA',
    'email_settings': {
        'send_log_mail_after_run': True,
        'from': 'server <henk-jan.van.reenen@nlhealthcareclinics.com>',
        'to': 'henk-jan.van.reenen@nlhealthcareclinics.com',
        'subject': 'blablaa',
        'msg': 'Hierbij de pyelt log\n\n\n(deze mail niet beantwoorden)\n\n'
    }
}



adresnl_config = {
    'sor_schema': 'sor_adresnl',
    'data_path': general_config['data_root'] + '/adres_nl/',
    'download_path' : 'C:/Users/r.wiegerinck/Downloads/',
    'active': False
}


dbc_config = {
    'sor_schema': 'sor_dbc',
    'data_path': general_config['data_root'] + '/dbc/20160101/',
    'active': False
}

manual_data_config = {
    'sor_schema': 'sor_manual',
    'data_path': general_config['data_root'] + '/manual/',
    'active': False
}

nictiz_config = {
    'sor_schema': 'sor_nictiz',
    'data_path': general_config['data_root'] + '/nictiz/',
    'use_scraping': True,
    'scrape_url': 'https://decor.nictiz.nl/decor/services/',
    'active': True
}

timeff_config = {
    'sor_schema': 'sor_timeff',
    'source_connection': 'oracle://NLHCBIT:'+ SimpleEncrypt.decode('pwd', 'wrfCtcK4wq7DiMOBw4XCmsKfw4TCusK1wpXCs8OEw5vDocOJwrfCp8K_wrrCucOC')+ '@10.249.1.115:1521/NLHP',
    'default_schema': 'MTDX',
    'active': False
}

vektis_agb_config = {
    'sor_schema': 'sor_vektis',
    'data_path': general_config['data_root'] + '/vektis/AGB/U095/',
    'convert_vektis_zips_to_csv': False,
    'active': False
}

vektis_uzovi_config = {
    'sor_schema': 'sor_vektis',
    'data_path': general_config['data_root'] + '/vektis/UZOVI/',
    'active': False
}

pipeline = Pipeline(general_config)
pipe = pipeline.get_or_create_pipe("nictiz", nictiz_config)


# ref_mapping = SorToRefMapping({'M': 'man', 'V': 'vrouw', 'O': 'onbekend'}, 'geslacht_types')
# pipe.mappings.append(ref_mapping)
#
# ref_mapping = SorToRefMapping({'9': 'patienten', '7': 'mdw'}, 'relatie_soorten')
# pipe.mappings.append(ref_mapping)

# todo maak een een hstage tabel om mee mee te testen.
mappings = []
ref_mapping = SorToRefMapping('valuesets_hstage', 'Adres soort')
ref_mapping.map_code_field('valuesets_hstage.id')
ref_mapping.map_descr_field('valuesets_hstage.valueset')
mappings.append(ref_mapping)
#
#

pipeline.run()
