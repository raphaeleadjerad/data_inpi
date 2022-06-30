# Donnees RCS

http://data.cquest.org/inpi_rncs/imr/Doc_Tech_IMR_Mai_2019_v1.5.1.pdf

L’INPI rediffuse des informations provenant  des  inscriptions  enregistrées  auprès  des greffes des tribunaux de commerce, des tribunaux d’Instance des départements du Bas-Rhin, du Haut-Rhin et de la Moselle, et des tribunaux mixtes de commerce des départements et régions d'outre-mer, telles  quetransmises  par  les  greffes  à  compter  du  5  mai  2017,  auxquelles s’ajoute désormais le stock des dossiers actifs (personnes morales et physiques).

Le contenu mis à disposition par l’INPI comprend:

-Les dossiers des données relatives aux personnes actives (morales et physiques):

-Un stock initial constitué àla date du 4 mai 2017pour les données issues des Tribunaux de commerce (TC)

-Un stock initial constitué àla date du 5 mai 2018pour les données issues des Tribunaux d’instance et Tribunaux mixtes de commerce (TI/TMC)

-Des stocks partiels constitués des dossiers complets relivrésà la demande de l’INPI après détection d’anomalies 

Les fichiers   des   données   contenuesdans   les nouvelles inscriptions (immatriculations, modifications  et  radiations) duRegistre  national  du  commerce  et  des  sociétés  ainsi  que  les informations  relatives  aux  dépôts  des  actes  et  comptes  annuels,  telles  que  transmises  par  les greffes à compter du 5 mai 2017(données de flux)

.Au total, ce sont les données d’environ 5 millions de personnes actives (morales et physiques)

Ces données sont mises à jour quotidiennement (environ 1,4 million de nouvelles inscriptions par an).

Les tribunaux représentés sont au nombre de 148 répartis comme suit (liste fournie en annexe): 
134 Tribunaux de commerce,
7 Tribunaux d’Instance des départements du Bas-Rhin, du Haut-Rhin et de la Moselle,
7 Tribunaux mixtes de commerce des départements et régions d'outre-mer.

On travaille sur les données de stocks (stocks initiaux et stocks partiels)
Les données sont organisées et zippées par greffe par année/mois/jour de la transmission. 

## Détail des données

 - Constitution du dossier d’une personne morale ou personne physique(identifiée par son siren et son n° de gestion), dans le cas d’une 1ère immatriculation,
 - Mise  à  jour des  informations  disponibles  sur  une personne  morale  ou  personne  physique en cas de modification ou de radiation

 Dans le cas d’une immatriculation (Personne morale ou Personne physique), le dossier est composé:
 - A minima, d’informations sur l’identité  de  la  personne (ex. date d’immatriculation, inscription principale   ou   secondaire,   dénomination,   nom,   prénom, forme   juridique,   capital, activité principale etc.)
 - Complété éventuellement par des informations relatives aux:
    - Représentants
    - Etablissements (établissement principal et/ou établissements secondaires)
    - Observations (incluant les procédures collectives, mentionsde radiation etc.)
    - Dépôt des comptes annuels
    - Dépôt des actes

En cas de mise à jour d’un dossier suite à un événement (modification, radiation), les fichiers transmis ont une structure identique aux fichiers créés à l’immatriculation  avec la  présence  de 2  champs spécifiques: la date de l’événement (Date_Greffe) et le libellé de l’événement (Libelle_Evt)

Dans ces cas, 6 types de fichiers supplémentaires, numérotés,sont transmis correspondant à:
- Evénements modifiant ou complétant les dossiers d’immatriculation des personnes morales (2) ou physiques (4) PM_EVT, PP_EVT
- Evénements  modifiant  ou  complétant  les  informations  relatives  aux  représentants  (6)  ou  aux établissements (9) ep_nouveau_modifie _EVT, ets_nouveau_modifie_EVT
- Evénements  supprimant  des  représentants  (7 –Représentant  partant)  ou  des  établissements (10 – Etablissement supprimé) ep_partant_EVT, ets_supprime_EVT.


## Identifiant des représentants

Pour les représentants on a la variable de Qualité (Gérant, Liquidateur, Associé), donc on connait pour chaque société le gérant. 

## Identification des SCI

On a la Forme juridique dans la table PM (variable obligatoirement renseignée). 
On a donc les siren des SCI, on peut vérifier qu'on en a le même nombre que dans les 2072. 