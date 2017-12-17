#WARNING: vse poizvedbe morajo biti filtrirane na to ali ima filem definiran budget in revenue
# Potrebujejo filter: num_unique, all_keywords, all_actors,
# Ne potrebujejo filtra
# column_types





# stevilo unikatnih vrstic v podanih stolpcih in tabelah
# vhod: dict {"ime_tabele1": ["stolpec1", "stolpec2"], "ime_tabele2": ["stolpec1"]}
# izhod: int
def num_unique(tables, columns):
    pass

# slovar imen stolpcev in pripadajoci podatkovni tipi vseh tabel
# vhod: /
# izhod: dict { "ime_tabele1": [["ime_stolpca1", "integer"], ["ime_stolpca2", "string"]], ...}
def column_types():
    pass


# seznam vseh igralcev
# vhod: /
# izhod: list [{id_igralca1: "ime_igralca1"}, {id_igralca2: "ime_igralca2"}]
def all_actors():
    pass

# seznam vseh kljucnih besed
# vhod: /
# izhod: list [{id_KB1: "besedilo_KB1"}, {id_KB2: "besedilo_KB2"}]
def all_keywords():
    pass




