from sqlalchemy import create_engine

from pyelt.mappings.sor_to_dv_mappings import SorToRefMapping
from pyelt.mappings.source_to_sor_mappings import SourceToSorMapping
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


def get_distinct_valueset():  # verzamel de verschillende valueset namen uit de hstage en stop ze in een list. gebruik
    #deze list om doorheen te lopen om iedere valueset in een aparte tabel te plaatsen. verander eventueel de naam (geen hoofdletters geen spaties maar underscores)
    sql = """Select distinct valueset
              from sor_nictiz.valuesets_hstage"""

    conn_string = general_config['conn_dwh']
    engine = create_engine(conn_string)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()

    result = cursor.fetchall()
    return result



def transform_valueset_name(valueset):
    """
    :param valueset:
    :return: tempstr; dit is de valueset naam maar dan ipv spaties een underscore en alles met kleine letters.
    """
    tempstr = str(valueset)
    tempstr = tempstr.lower()
    tempstr = tempstr.replace("('", "")
    tempstr = tempstr.replace("',)", "")
    tempstr = tempstr.replace(" ", "_")
    tempstr = tempstr.replace("-", "")
    tempstr = tempstr.replace("__", "_")
    tempstr = tempstr.replace("(", "")
    tempstr = tempstr.replace(")", "")
    return tempstr

"""deze versie "werkt" op 1 tabel"""
# def init_sor_to_ref_mappings(pipe):
#     mappings = []
#     ref_mapping = SorToRefMapping('valuesets_hstage', 'adres_soort')  # ipv 'Adres soort'
#     ref_mapping.map_code_field('valuesets_hstage.code')
#     ref_mapping.map_descr_field('valuesets_hstage.displayname')
#     ref_mapping.map_level_field('valuesets_hstage.niveau')
#     ref_mapping.map_leveltype_field('valuesets_hstage.niveau_type')
#     mappings.append(ref_mapping)
#     return mappings

"""verder testen:"""
def init_sor_to_ref_mappings(pipe):
    mappings = []
    sor = pipe.sor
    valuesets = get_distinct_valueset()
    for valueset in valuesets:  # valueset is de naam van de valueset in de kolom "valueset" van de hstage; kan oa komma's en hoofdletters bevatten
        valueset_name = transform_valueset_name(valueset)  # in valueset_name zijn oa spaties eruit gehaald door de functie "transform_valueset_name"
        valueset_kind = str(valueset)
        valueset_kind = valueset_kind.replace("('", "")
        valueset_kind = valueset_kind.replace("',)", "")


# adresnl_filter = "update_type = 'delete'"
#     mapping = SorToEntityMapping('adresnl_hstage', AdresNL, sor, filter=adresnl_filter)
        sql_string = 'valueset = "{}"'.format(valueset_kind)
        ref_filter = sql_string
        ref_mapping = SorToRefMapping('valuesets_hstage', '{}'.format(valueset_name), sor, filter=ref_filter)  # ipv 'Adres soort'
        ref_mapping.map_code_field('valuesets_hstage.code')
        ref_mapping.map_descr_field('valuesets_hstage.displayname')
        ref_mapping.map_level_field('valuesets_hstage.niveau')
        ref_mapping.map_leveltype_field('valuesets_hstage.niveau_type')

        mappings.append(ref_mapping)
    return mappings


pipeline = Pipeline(general_config)
pipe = pipeline.get_or_create_pipe("nictiz", nictiz_config)
"""let op! ref_schema_tester maakt gebruik van een reeds gemaakte sor laag """
# mappings = SourceToSorMapping('valueset_hstage',pipe)
# pipe.mappings.extend(mappings)
pipe.mappings.extend(init_sor_to_ref_mappings(pipe))



"""alternatief voor ref_mapping; via een opgegeven dictionary:"""
# ref_mapping = SorToRefMapping({'M': 'man', 'V': 'vrouw', 'O': 'onbekend'}, 'geslacht_types')
# pipe.mappings.append(ref_mapping)
#
# ref_mapping = SorToRefMapping({'9': 'patienten', '7': 'mdw'}, 'relatie_soorten')
# pipe.mappings.append(ref_mapping)





#
#

pipeline.run()
