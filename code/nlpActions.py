import spacy


target_entity_labels = ["PERSON", "NORP", "FAC", "ORG", "GPE", "LOC", "PRODUCT", "EVENT", "LAW"]
   
#Spacy nlp pipeline with only parts needed for NER
nlp_ner_only = spacy.load("en_core_web_md")


def get_target_entities(text) :
    """
    Return detected entities of interest (see list) and total entity count

    Target Entities:
    PERSON,
    NORP (nationalities, religious and political groups),
    FAC (buildings, airports etc.),
    ORG (organizations),
    GPE (countries, cities etc.),
    LOC (mountain ranges, water bodies etc.),
    PRODUCT (products),
    EVENT (event names),
    LAW (legal document titles)
    """
    target_entity_dict = {"PERSON": set(),
                          "NORP": set(), 
                          "FAC": set(),
                          "ORG": set(),
                          "GPE": set(),
                          "LOC": set(),
                          "PRODUCT": set(),
                          "EVENT": set(),
                          "LAW": set()}

    doc = nlp_ner_only(text)
    ent_count = 0

    for ent in doc.ents:
        if target_entity_labels.__contains__(ent.label_):
            ent_count += 1
            target_entity_dict.get(ent.label_).add(ent.text)
    
    for label in target_entity_labels:
        target_entity_dict[label] = list(target_entity_dict[label])

    target_entity_dict["entityMentions"] = ent_count
    return target_entity_dict

