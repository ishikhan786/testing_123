from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from typing import List, Optional
from pydantic import BaseModel, Field
import uvicorn
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from dateutil.parser import parse as parse_date, ParserError
from rapidfuzz import fuzz
from pymongo import MongoClient


try:
    client = MongoClient("")
    db = client["AML_EXIM_BANK"]
    collection = db["Adverse_media_3"]
except Exception as e:
    raise Exception(f"MongoDB connection failed: {e}")

app = FastAPI("Adverse Media API")

FUZZY_MATCH_THRESHOLD = 75


class RiskAssessment(BaseModel):
    name: str
    status: str
    confidence: Optional[int] = None
    justification: Optional[List[str]] = None


class PersonRecord(BaseModel):
    url: str
    title: str
    description: str
    date: Optional[str]
    persons: List[str]
    organizations: List[str]
    risk_assessment: Optional[List[RiskAssessment]]


def is_fuzzy_match(query_name: str, target_name: str, threshold: int = FUZZY_MATCH_THRESHOLD) -> bool:
    query_tokens = set(query_name.lower().strip().split())
    target_tokens = set(target_name.lower().strip().split())

    # Exact token subset match
    if query_tokens.issubset(target_tokens):
        return True

    # Different fuzzy matching strategies
    score_partial = fuzz.partial_ratio(query_name, target_name)
    score_token_sort = fuzz.token_sort_ratio(query_name, target_name)
    score_token_set = fuzz.token_set_ratio(query_name, target_name)
    score_reverse = fuzz.ratio(query_name[::-1], target_name[::-1])
    score_substring = 100 if query_name in target_name or target_name in query_name else 0

    return max(score_partial, score_token_sort, score_token_set, score_reverse, score_substring) >= threshold


def filter_person_data_only(article, target_name: str):
    """
    Keep only relevant persons/organizations from article that match target_name.
    """
    persons = article.get("persons", [])
    organizations = article.get("organizations", [])

    matched_persons = [p for p in persons if is_fuzzy_match(target_name, p)]
    matched_orgs = [o for o in organizations if is_fuzzy_match(target_name, o)]

    article["persons"] = matched_persons
    article["organizations"] = matched_orgs

    return article

@app.get("/person")
def get_person_articles(name: str = Query(...)):
    raw_results = list(collection.find({
        "$or": [
            {"persons": {"$regex": name, "$options": "i"}},
            {"organizations": {"$regex": name, "$options": "i"}}
        ]
    }, {
        "_id": 0,
        "url": 1,
        "title": 1,
        "date": 1,
        "persons": 1,
        "organizations": 1
    }))

    processed_results = []

    for record in raw_results:
        # ✅ Apply fuzzy filter
        record = filter_person_data_only(record, name)
        if record["persons"] or record["organizations"]:
            try:
                dt = parse_date(record.get("date", ""))
                record["parsed_date"] = dt.replace(tzinfo=None)
            except (TypeError, ValueError, ParserError):
                record["parsed_date"] = datetime.min

            # remove persons & organizations before appending
            record.pop("persons", None)
            record.pop("organizations", None)

            processed_results.append(record)

    # ✅ Sort once
    processed_results.sort(key=lambda x: x["parsed_date"], reverse=True)

    # ✅ Remove helper field
    for article in processed_results:
        article.pop("parsed_date", None)

    # ✅ Response
    if processed_results:
        return JSONResponse({
            "ResponseStatus": "MATCHES",
            "articles": processed_results
        })
    else:
        return JSONResponse({
            "ResponseStatus": "CLEAR",
            "details": f"No data found for {name}"
        })



if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0",port=8002)