import pandas as pd
from neo4j import GraphDatabase
from Neo4jCrediental import *
import os

#insert your credential
URI="neo4j://localhost:7687"
NAME="neo4j"
PASSWORD=""

dfs=os.listdir("Datasets")[:11]
URI = URI
AUTH = (NAME,PASSWORD)

def preprocessing(name, years, genres, companies, countrys, info, infoquery): 
    if years != []:  
        if len(years)==1:
            year=years[0][-4:]
            records, summary, keys = driver.execute_query("""
                    MERGE (movie:Movie {name:$n})
                    MERGE (year:year {year: $y})
                    MERGE (movie)-[:RELEASED]->(year)
                    """, 
                    y=year, 
                    n=name,
                    database_="neo4j",
                )
        else:
            if years[1] !=' ':
                records, summary, keys = driver.execute_query("""
                    MERGE (series:Series {name:$n})
                    MERGE (yearS:year {year: $yS})
                    MERGE (yearF:year {year: $yF})
                    MERGE (series)-[:RELEASED]->(yearS)
                    MERGE (series)-[:ENDED]->(yearF)
                    """, 
                    yS=years[0],
                    yF=years[1], 
                    n=name,
                    database_="neo4j",
                )
            else:
                records, summary, keys = driver.execute_query("""
                    MERGE (series:Series {name:$n})
                    MERGE (yearS:year {year: $yS})
                    MERGE (series)-[:RELEASED]->(yearS)
                    """, 
                    yS=years[0],
                    n=name,
                    database_="neo4j",
                )
    records, summary, keys = driver.execute_query(query_=infoquery, parameters_=info, database_="neo4j",)
    if genres !=[]:
        for genre in genres:
            records, summary, keys = driver.execute_query("""
                MATCH (item {name:$n})
                MERGE (genre:genre {genre:$g})
                MERGE (item)-[:GENRE]-> (genre)
                """, 
                n=name,
                g=genre,
                database_="neo4j",
            ) 
    if companies !=[]:
        for company in companies:
            records, summary, keys = driver.execute_query("""
                MATCH (item {name:$n})
                MERGE (comp:company {company:$c})
                MERGE (item)-[:PRODUCTED_BY]-> (comp)
                """, 
                n=name,
                c=company,
                database_="neo4j",
            ) 
    if countrys !=[]:
        for country in countrys:
            records, summary, keys = driver.execute_query("""
                MATCH (item {name:$n})
                MERGE (country:country {country:$c})
                MERGE (item)-[:PRODUCTED_IN]-> (country)
                """, 
                n=name,
                c=country,
                database_="neo4j",
            ) 

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    records, summary, keys = driver.execute_query("""
        MATCH (p)
        DETACH DELETE p
        """, 
        database_="neo4j",
    )
    for path in dfs:
        n=int(path.split(" ")[0])
        path="Datasets/"+path
        print(n)
        if(n==1):
            df = pd.read_csv(path)
            df=df.drop(columns=["id", "status", "adult", "backdrop_path", "homepage", "imdb_id", "original_title", "poster_path", "spoken_languages"])
            df=df.to_dict("index")
            for item in df.values():
                
                if not pd.isna(item["title"]):
                    name=item["title"].lower().strip()
                else:
                    continue
                
                if not pd.isna(item["release_date"]):
                    years=[item["release_date"][:4]]
                else:
                    years=[]
                
                genres=item["genres"].split(", ")
                genres=[i.lower().strip() for i in genres]
                
                if not pd.isna(item["production_companies"]):
                    companies=item["production_companies"].split(", ")
                    companies=[i.lower().strip() for i in companies]
                else:
                    companies=[]
                
                if not pd.isna(item["production_countries"]):
                    countrys=item["production_countries"].split(", ")
                    countrys=[i.lower().strip() for i in countrys]
                else:
                    countrys=[]
                
                if  not pd.isna(item["vote_average"]):
                    rating=item["vote_average"]
                else:
                    rating=None
                    
                if  not pd.isna(item["vote_count"]):
                    votes=item["vote_count"]
                else:
                    votes=None
                
                info={"n": name, "r":rating, "v":votes, "rev":item["revenue"], "m":item["runtime"], "b":item["budget"], "l":item["original_language"],
                      "ow":item["overview"], "p":item["popularity"], "t":item["tagline"]}
                infoquery= """
                MATCH (item {name:$n})
                CREATE (item)-[:INFO]->(info:information {min:$m, rating:$r, votes:$v, revenue:$rev, budget:$b, language:$l, description:$ow, popularity:$p, tagline:$t})
                """
                preprocessing(name, years, genres, companies, countrys, info, infoquery)
        elif(n==2):
            df = pd.read_csv(path)
            df=df.to_dict("index")
            for item in df.values():
                name=item["Name"].lower().strip()
                
                years=item["Span"].strip("()")
                years=years.split("–")
                if len(years)==1:
                    years.append(' ')
                    
                if  not pd.isna(item["Rating"]):
                    rating=item["Rating"]
                else:
                    rating=None
                
                info={"n":name, "ow":item["Description"], "r":rating}
                infoquery= """
                MATCH (item {name:$n})
                CREATE (item)-[:INFO]->(info:information {rating:$r, description:$ow})
                """
                preprocessing(name, years, [], [], [], info, infoquery)
        elif(n==3):
            df = pd.read_csv(path)
            df=df.to_dict("index")
            for item in df.values():
                name=item["Anime"].lower().strip()
                
                years=item["Release_date"].strip("()")
                years=years.split("–")
                if len(years)==1:
                    years.append(' ')
                
                genres=item["Genre"].split(", ")
                genres=[i.lower().strip() for i in genres]
                
                if not pd.isna(item["Length"]):
                    min=int(item["Length"].replace(",","").split()[0])
                else:
                    min=None
                    
                if not pd.isna(item["Rating"]):
                    rating=item["Rating"]
                else:
                    rating=None
                
                info={"n":name, "m":min, "r":rating}
                infoquery= """
                MATCH (item {name:$n})
                CREATE (item)-[:INFO]->(info:information {min: $m, rating:$r})
                """
                preprocessing(name, years, genres, [], [], info, infoquery)
        elif(n==4):
            df = pd.read_csv(path)
            df=df.to_dict("index")
            for item in df.values():
                company=item["Studio"]
                
                country=item["Country"]
                
                year=item["Founded"]
                records, summary, keys = driver.execute_query("""
                    MERGE (comp:company {company:$comp})
                    MERGE (country:country {country:$c})
                    MERGE (year:year {year:$y})
                    MERGE (comp)-[:SITE_IN]-> (country)
                    MERGE (comp)-[:FOUNDED]-> (years)
                    """, 
                    comp=company,
                    c=country,
                    y=year,
                    database_="neo4j",
                ) 
        elif(n==5):
            df = pd.read_csv(path)
            df=df.drop(columns=["Stars", "Certificate", "Metascore", "Episode", "Episode Title"])
            df=df.to_dict("index")
            names=[]
            for item in df.values():
                name=item["Title"].lower().strip()
                if name not in names:
                    names.append(name)
                    if not pd.isna(item["Year"]):
                        years=item["Year"].strip("()")
                        years=years.split("–")
                        years[0]=''.join(c for c in years[0] if c.isdigit())
                    else:
                        years=[]
                        
                    if not pd.isna(item["Runtime"]) and item["Runtime"] != 'Runtime':
                        min=int(item["Runtime"].replace(",","").split()[0])
                    else:
                        min=None
                        
                    if not pd.isna(item["User Rating"]):
                        Rating=item["User Rating"]
                    else:
                        Rating=None
                        
                    if not pd.isna(item["Number of Votes"]):
                        votes=item["Number of Votes"]
                    else:
                        votes=None
                    
                    genres=item["Genre"].split(", ")
                    genres=[i.lower().strip() for i in genres]
                    
                    info={"n": name, "r":Rating, "v":votes, "m":min, "rev":item["Gross"], "ow":item["Summary"], }
                    infoquery= """
                    MATCH (item {name:$n})
                    CREATE (item)-[:INFO]->(info:information {min:$m, rating:$r, votes:$v, revenue:$rev, description:$ow})
                    """
                    preprocessing(name, years, genres, [], [], info, infoquery)   
        elif(n==6):
            subpaths=os.listdir(path)
            dfs=[]
            for subpath in subpaths:
                dfs.append(pd.read_csv(path+"/"+subpath))

            df1=dfs[0].to_dict("index")
            for item in df1.values():
                
                name1=item["Title"].lower().strip()
                
                years=[]
                years.append(item["Premiere Year"])
                years.append(item["Final Year"])
                
                if not pd.isna(item["Country"]):
                    countrys=item["Country"].split(", ")
                    countrys=[i.lower().strip() for i in countrys]
                else:
                    countrys=[]
                    
                info={"n": name1, "e":item["Episodes"]}
                infoquery= """
                    MATCH (item {name:$n})
                    CREATE (item)-[:INFO]->(info:information {epiodies:$e})
                    """
                preprocessing(name1, years, [], [], countrys, info, infoquery)
                
            df2=dfs[1].to_dict("index")
            for item in df2.values():
                name2=item["Title"].lower().strip()
                
                years=[]
                years.append(item["Premiere Year"])
                years.append(item["Final Year"])
                
                if not pd.isna(item["Country"]):
                    countrys=item["Country"].split(", ")
                    countrys=[i.lower().strip() for i in countrys]
                else:
                    countrys=[]
                    
                info={"n": name1, "e":item["Episodes"], "s":item["Seasons"], "c":item["Original Channel"], "t":item["Technique"]}
                infoquery= """
                    MATCH (item {name:$n})
                    CREATE (item)-[:INFO]->(info:information {epiodies:$e, seasons:$s, channel:$c, technique:$t})
                    """
                preprocessing(name2, years, [], [], countrys, info, infoquery)
                
            df3=dfs[2].to_dict("index")
            for item in df3.values():
                name3=item["Title"].lower().strip()
                years=[item["Year"], ' ']

                
                if not pd.isna(item["Country"]):
                    countrys=item["Country"].split(", ")
                    countrys=[i.lower().strip() for i in countrys]
                else:
                    countrys=[]
                    
                info={"n": name1, "e":item["Episodes"], "t":item["Technique"]}
                infoquery= """
                    MATCH (item {name:$n})
                    CREATE (item)-[:INFO]->(info:information {epiodies:$e, technique:$t})
                    """
                preprocessing(name2, years, [], [], countrys, info, infoquery)
        elif(n==7):
            df = pd.read_csv(path)
            df=df.drop(columns=["certificate"])
            df=df.to_dict("index")
            for item in df.values():
                name=item["title"].lower().strip()
                
                if not pd.isna(item["year"]):
                    years=item["year"].strip("()")
                    years=years.split("–")
                else:
                    years=[]
                    
                genres=item["genre"].split(", ")
                genres=[i.lower().strip() for i in genres]
                
                if not pd.isna(item["runtime"]) and item["runtime"]:
                    min=item["runtime"].split()[0]
                else:
                    min=None
                    
                if not pd.isna(item["rating"]):
                    Rating=item["rating"]
                else:
                    Rating=None
                                        
                if not pd.isna(item["votes"]):
                    votes=item["votes"]
                else:
                    votes=None
                
                info={"n": name, "r":Rating, "v":votes, "m":min, "ow":item["desc"]}
                infoquery= """
                MATCH (item {name:$n})
                CREATE (item)-[:INFO]->(info:information {min:$m, rating:$r, votes:$v, description:$ow})
                """
                preprocessing(name, years, genres, [], [], info, infoquery)
        elif(n==8):
            df = pd.read_csv(path)
            df=df.to_dict("index")
            for item in df.values():
                
                years=item["Year"].strip("()")
                years=years.split("–")
        
                name=item["Name"].lower().strip()
                
                if not pd.isna(item["Minutes"]):
                    min=int(item["Minutes"].split()[0])
                else:
                    min=None
                    
                genres=item["genre"].split(", ")
                genres=[i.lower().strip() for i in genres]
                
                rating=float(item["Rating"])
                
                if not pd.isna(item["Votes"]):
                    votes=int(item["Votes"].replace(",",""))
                else:
                    votes=None    
                
                info = {"n": name, "m": min, "r":rating, "v":votes}
                infoquery="""
                MATCH (item {name:$n})
                CREATE (item)-[:INFO]->(info:information {min:$m, rating:$r, votes:$v})
                """
                
                preprocessing(name, years, genres, [], [], info, infoquery)
        elif(n==9):
            df = pd.read_csv(path)
            df=df.drop(columns=["Metascore","Certificate"])
            df=df.to_dict("index")
            for item in df.values():
                name=item["Title"].lower().strip()
                
                years=[str(item["Year"])]
                
                genres=item["Genre"].split(", ")
                genres=[i.lower().strip() for i in genres]
                
                if not pd.isna(item["Runtime"]):
                    min=int(item["Runtime"].split()[0])
                else:
                    min=None
                    
                if not pd.isna(item["Rating"]):
                    Rating=item["Rating"]
                else:
                    Rating=None
                                        
                if not pd.isna(item["Votes"]):
                    votes=item["Votes"]
                else:
                    votes=None
                
                info={"n": name, "r":Rating, "v":votes, "rev":item["Gross"], "m":min, "ow":item["Description"], "d":item["Director"]}
                infoquery= """
                MATCH (item {name:$n})
                CREATE (item)-[:INFO]->(info:information {min:$m, rating:$r, votes:$v, revenue:$rev, description:$ow, director:$d})
                """
                preprocessing(name, years, genres, [], [], info, infoquery)