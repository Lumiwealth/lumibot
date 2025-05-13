import json
import datetime
import time
import logging
import re
from openai import OpenAI

# --- constants ---------------------------------------------------------------
_MODEL_LIMITS = {
    "sonar": 8000,
    "sonar-pro": 8000,
    "sonar-reasoning": 8000,
    "sonar-reasoning-pro": 8000,
    "sonar-deep-research": 8000,
}
_REASONING_MODELS = {"sonar-reasoning", "sonar-reasoning-pro"}

# --- utilities ---------------------------------------------------------------
def _strip_think_block(text: str) -> str:
    """Remove the <think>…</think> preamble inserted by reasoning models."""
    if text.startswith("<think>"):
        return text.split("</think>", 1)[-1].lstrip()
    return text

def _build_response_format(schema_dict: dict) -> dict:
    """Return the Perplexity response_format payload for strict JSON."""
    return {"type": "json_schema", "json_schema": {"schema": schema_dict}}

# --- Formal JSON Schema Definitions ------------------------------------------
FINANCIAL_NEWS_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "query": {"type": "string"},
        "analysis_summary": {"type": "string"},
        "items": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string"},
                    "asset_type": {"type": "string"},
                    "headline": {"type": "string"},
                    "confidence": {"type": "integer"},
                    "sentiment_score": {"type": "integer"},
                    "popularity_metric": {"type": "integer"},
                    "volume_of_messages": {"type": ["integer", "null"]},
                    "magnitude": {"type": "integer"},
                    "type_of_news": {"type": ["string", "null"]},
                    "price_targets": {
                        "type": ["object", "null"],
                        "properties": {
                            "low": {"type": ["number", "null"]},
                            "high": {"type": ["number", "null"]},
                            "average": {"type": ["number", "null"]}
                        },
                        "additionalProperties": False
                    },
                    "additional_info": {
                        "type": ["object", "null"],
                        "properties": {
                            "sector": {"type": ["string", "null"]},
                            "recent_events": {"type": ["string", "null"]},
                            "notable_executive_actions": {"type": ["string", "null"]},
                            "macro_support": {"type": ["string", "null"]},
                            "related_tickers": {"type": "array", "items": {"type": "string"}},
                            "external_links": {"type": "array", "items": {"type": "string"}}
                        },
                        "additionalProperties": False
                    }
                },
                "required": ["symbol", "asset_type", "headline", "confidence", "sentiment_score", "popularity_metric", "magnitude"],
                "additionalProperties": False
            }
        }
    },
    "required": ["query", "analysis_summary", "items"],
    "additionalProperties": False
}

DEFAULT_GENERAL_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "query": {"type": "string"},
        "response_summary": {"type": "string"},
        "detailed_response": {"type": ["string", "null"]},
        "symbols": {
            "type": "array",
            "items": {"type": "string"}
        }
    },
    "required": ["query", "response_summary", "symbols"],
    "additionalProperties": False
}

class PerplexityHelper:
    """
    A helper for querying Perplexity's API via an OpenAI-compatible client.
    
    Supports two types of queries:
      - Financial news queries: Returns structured financial news data.
      - General queries: Returns structured responses based on a provided (or default) JSON schema.
    """
    
    def __init__(self, api_key: str):
        """
        Initializes the PerplexityHelper with your API key and creates an OpenAI-compatible client.
        
        Parameters
        ----------
        api_key : str
            Your Perplexity API key. If not provided, the environment variable 'PERPLEXITY_API_KEY'
            will be used.
        
        Raises
        ------
        ValueError
            If no API key is provided or found in the environment.
        """
        if not api_key or api_key.lower() == 'your_api_key_here':
            import os
            api_key = os.getenv("PERPLEXITY_API_KEY")
            if not api_key:
                raise ValueError("API key is required for PerplexityHelper. Set it as PERPLEXITY_API_KEY in your environment or your secrets")
        
        self.api_key = api_key
        self.client = OpenAI(
            api_key=self.api_key,
            base_url="https://api.perplexity.ai",  # Correct base URL
        )
    
    # --------------------------------------------------------------------------
    # Private Helper Methods
    # --------------------------------------------------------------------------
    def _clean_response(self, response_text: str) -> str:
        """
        Cleans the raw response by removing markdown code fences and any extraneous text 
        preceding the first '{', so that only a valid JSON object remains.
        
        Parameters
        ----------
        response_text : str
            The raw response text from the API.
        
        Returns
        -------
        str
            The cleaned response text.
        """
        response_text = response_text.strip()
        # Remove markdown code fences if present
        if response_text.startswith("```"):
            lines = response_text.splitlines()
            filtered = [line for line in lines if not re.match(r"^```", line.strip())]
            response_text = "\n".join(filtered).strip()
        # Remove any leading text before the first '{'
        first_brace = response_text.find("{")
        if first_brace != -1:
            response_text = response_text[first_brace:]
        return response_text

    def _build_financial_news_prompt(self, user_query: str) -> str:
        """
        Builds a system prompt for financial news queries.
        
        The prompt instructs the model to output ONLY valid JSON (with no markdown or extra text) 
        following the schema below.
        
        Schema:
        
            {
              "query": "<string, echo the user's query>",
              "analysis_summary": "<string, a concise summary of the news findings>",
              "items": [
                {
                  "symbol": "<string, for example 'AAPL' or 'TSLA'>",
                  "asset_type": "<string, for example 'stock', 'crypto', 'index', etc.>",
                  "headline": "<string, a brief headline summarizing the news>",
                  "confidence": "<integer, a score from 0 to 10 representing reliability>",
                  "sentiment_score": "<integer, score from -10 (very bearish) to 10 (very bullish)>",
                  "popularity_metric": "<integer, non-negative count of mentions>",
                  "volume_of_messages": "<integer, optional, representing the volume of related communications>",
                  "magnitude": "<integer, a score from 0 to 10 indicating overall impact>",
                  "type_of_news": "<string, optional, e.g. 'earnings', 'ipo', 'macro'>",
                  "price_targets": {
                    "low": "<float, optional, the lowest recommended price target>",
                    "high": "<float, optional, the highest recommended price target>",
                    "average": "<float, optional, the calculated average target price>"
                  },
                  "additional_info": {
                    "sector": "<string, optional, e.g. 'technology' or 'finance'>",
                    "recent_events": "<string, optional, key events related to the asset>",
                    "notable_executive_actions": "<string, optional, e.g. 'CEO resignation' or 'merger announcement'>",
                    "macro_support": "<string, optional, description of macroeconomic factors affecting the asset>",
                    "related_tickers": ["<string, list of ticker symbols for similar companies>"],
                    "external_links": ["<string, URL linking to detailed news articles or official reports>"]
                  }
                }
              ]
            }
        
        Parameters
        ----------
        user_query : str
            The financial news query.
        
        Returns
        -------
        str
            The system prompt to be sent to the API.
        """
        system_prompt = f"""\
You are a financial news aggregator assistant with real-time access to news feeds and financial data via Perplexity.
Remain factual and accurate. Do not include any markdown formatting or extra text.

Return only the JSON object (with no preamble or explanation) following this exact schema:

{{
  "query": "<string, echo the user's query>",
  "analysis_summary": "<string, a concise summary of the news findings>",
  "items": [
    {{
      "symbol": "<string, for example 'AAPL' or 'TSLA'>",
      "asset_type": "<string, for example 'stock', 'crypto', 'index', etc.>",
      "headline": "<string, a brief headline summarizing the news>",
      "confidence": "<integer, a score from 0 to 10 representing reliability>",
      "sentiment_score": "<integer, score from -10 (very bearish) to 10 (very bullish)>",
      "popularity_metric": "<integer, non-negative count of mentions>",
      "volume_of_messages": "<integer, optional, representing the volume of related communications>",
      "magnitude": "<integer, a score from 0 to 10 indicating overall impact>",
      "type_of_news": "<string, optional, e.g. 'earnings', 'ipo', 'macro'>",
      "price_targets": {{
        "low": "<float, optional, the lowest recommended price target>",
        "high": "<float, optional, the highest recommended price target>",
        "average": "<float, optional, the calculated average target price>"
      }},
      "additional_info": {{
        "sector": "<string, optional, e.g. 'technology' or 'finance'>",
        "recent_events": "<string, optional, key events related to the asset>",
        "notable_executive_actions": "<string, optional, e.g. 'CEO resignation' or 'merger announcement'>",
        "macro_support": "<string, optional, description of macroeconomic factors affecting the asset>",
        "related_tickers": ["<string, list of ticker symbols for similar companies>"],
        "external_links": ["<string, URL linking to detailed news articles or official reports>"]
      }}
    }}
  ]
}}

Instructions:
1) Output MUST be valid JSON (with no extra text or markdown).
2) Do not include any preamble or explanation.
3) The 'query' field must exactly echo the user's query.

Now, the user's query is:
\"{user_query}\"

Return only valid JSON following the schema.
"""
        return system_prompt

    def _build_general_prompt(self, user_query: str, custom_schema=None) -> str:
        """
        Constructs a system prompt for general queries.
        
        You can provide a custom JSON schema as either a Python dictionary or a string.
        It is recommended that the schema thoroughly describes the expected JSON output, including detailed explanations for each field.
        
        The default schema (if no custom schema is provided) is:
        
            {
              "query": "<string, echo the user's query>",
              "response_summary": "<string, brief answer (1-3 sentences)>",
              "detailed_response": "<string, optional extended details>",
              "symbols": ["<string, list of relevant symbols>"]
            }
        
        A sample custom schema (different from the default) might be:
        
            {
              "query": "<string, echo the user's query>",
              "stocks": [
                {
                  "symbol": "<string, ticker symbol>",
                  "earnings_growth": "<float, earnings growth percentage>",
                  "analyst_rating": "<float, average analyst rating from 1 to 5>",
                  "price_target": "<float, consensus price target in USD>"
                }
              ],
              "summary": "<string, overall summary of findings>"
            }
        
        Additionally, instruct the model to return only the JSON object with no extra text or markdown.
        
        Parameters
        ----------
        user_query : str
            The user's query.
        custom_schema : dict or str, optional
            The desired JSON schema for the response.
        
        Returns
        -------
        str
            The system prompt to be sent to the API.
        """
        if custom_schema is None:
            schema = {
                "query": "<string, echo the user's query>",
                "response_summary": "<string, brief answer (1-3 sentences)>",
                "detailed_response": "<string, optional extended details>",
                "symbols": ["<string, list of relevant symbols>"]
            }
        elif isinstance(custom_schema, dict):
            schema = custom_schema
        else:
            schema = custom_schema

        if isinstance(schema, dict):
            schema_str = json.dumps(schema, indent=2)
        else:
            schema_str = schema

        system_prompt = f"""\
You are a knowledgeable assistant with access to real-time information via Perplexity.
Answer the user's query accurately and concisely, avoiding hallucinations.
Return only the JSON object (with no extra text or markdown) following the schema exactly.
Include detailed descriptions for each field so that the expected output is unambiguous.

The JSON schema is as follows:

{schema_str}

Instructions:
1) Output MUST be valid JSON (with no extra text or markdown).
2) Do not include any preamble or explanation.
3) The 'query' field must exactly echo the user's query.

Now, the user's query is:
\"{user_query}\"

Return only valid JSON following the schema.
"""
        return system_prompt

    def _send_request(
        self,
        system_msg: str,
        user_query: str,
        model: str = "sonar",
        temperature: int = 0,
        retries: int = 3,
        max_tokens: int = 35000,
        stream: bool = None,
        schema: dict = None
    ) -> str:
        """
        Sends a request to the Perplexity API using the provided system message and user query.
        Implements a retry loop to mitigate transient failures.
        Additional parameters like 'max_tokens' and 'stream' are included to encourage complete output.
        """
        # Clamp max_tokens to model limit
        safe_max_tokens = min(max_tokens, _MODEL_LIMITS.get(model, 8000))
        # Enable streaming by default for big answers
        if stream is None:
            stream = safe_max_tokens >= 4000
        # Build response_format if schema is provided
        response_format = None
        if schema:
            response_format = _build_response_format(schema)
        for attempt in range(1, retries + 1):
            try:
                payload = {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": user_query}
                    ],
                    "temperature": temperature,
                    "max_tokens": safe_max_tokens,
                    "top_p": 0.9,
                    "stream": stream,
                }
                if response_format:
                    payload["response_format"] = response_format
                if stream:
                    response_chunks = self.client.chat.completions.create(**payload)
                    response_text = ""
                    for chunk in response_chunks:
                        delta = getattr(chunk.choices[0], "delta", None)
                        if delta and getattr(delta, "content", None):
                            response_text += delta.content
                else:
                    completion = self.client.chat.completions.create(**payload)
                    if (not completion.choices or
                        not hasattr(completion.choices[0], "message") or
                        not hasattr(completion.choices[0].message, "content")):
                        raise ValueError("Invalid response structure from API.")
                    response_text = completion.choices[0].message.content
                if not response_text.strip():
                    raise ValueError("Received empty response from API.")
                # Strip <think> block for reasoning models
                if model in _REASONING_MODELS:
                    response_text = _strip_think_block(response_text)
                return response_text
            except Exception as e:
                logging.error(f"Attempt {attempt} failed: {e}")
                if attempt == retries:
                    logging.error(f"Final attempt failed. System prompt was: {system_msg}")
                    raise e
                time.sleep(1)
        raise RuntimeError("Failed to get a valid response after retries.")

    # --------------------------------------------------------------------------
    # Public Methods for Executing Queries
    # --------------------------------------------------------------------------
    def execute_financial_news_query(self, user_query: str) -> dict:
        """
        Executes a financial news query.
        
        This method performs the following steps:
        
        1. **Prompt Construction:**  
           Builds the prompt using the `_build_financial_news_prompt` method.
        
        2. **API Request:**  
           Sends the query to the Perplexity API using the `_send_request` method, which includes retry logic.
        
        3. **Response Parsing:**  
           Cleans and parses the returned JSON into a Python dictionary. If JSON decoding fails,
           logs the raw cleaned response and returns a dictionary with an error message in 'analysis_summary'
           and an empty 'items' list.
        
        Parameters
        ----------
        user_query : str
            The financial news query.
        
        Returns
        -------
        dict
            A dictionary containing the API's response following the financial news JSON schema.
        """
        system_msg = self._build_financial_news_prompt(user_query)
        try:
            assistant_text = self._send_request(system_msg, user_query, schema=FINANCIAL_NEWS_JSON_SCHEMA)
        except Exception as e:
            return {
                "query": user_query,
                "analysis_summary": f"Error calling Perplexity API: {str(e)}",
                "items": []
            }
        
        # Clean the raw response to remove markdown formatting and extraneous text
        cleaned_text = self._clean_response(assistant_text)
        try:
            data = json.loads(cleaned_text)
        except json.JSONDecodeError as e:
            logging.error(f"JSON decoding failed. Raw response: {cleaned_text}")
            return {
                "query": user_query,
                "analysis_summary": f"Error: LLM output was not valid JSON. {str(e)}",
                "items": []
            }
        
        self._post_process_data(data)
        return data

    def execute_general_query(self, user_query: str, custom_schema=None, model: str = "sonar", max_tokens: int = 35000, stream: bool = None) -> dict:
        """
        Executes a general query using the Perplexity API.
        
        This method performs the following steps:
        
        1. **Prompt Construction:**  
           Builds a system prompt using the `_build_general_prompt` method. You may supply a custom JSON schema 
           (as a dict or a string) that thoroughly describes the expected JSON output. If no custom schema is provided, 
           the default schema is used. The default schema is:
        
               {
                 "query": "<string, echo the user's query>",
                 "response_summary": "<string, brief answer (1-3 sentences)>",
                 "detailed_response": "<string, optional extended details>",
                 "symbols": ["<string, list of relevant symbols>"]
               }
        
           A sample custom schema (different from the default) might be:
        
               {
                 "query": "<string, echo the user's query>",
                 "stocks": [
                   {
                     "symbol": "<string, ticker symbol>",
                     "earnings_growth": "<float, earnings growth percentage>",
                     "analyst_rating": "<float, average analyst rating from 1 to 5>",
                     "price_target": "<float, consensus price target in USD>"
                   }
                 ],
                 "summary": "<string, overall summary of findings>"
               }
        
        2. **API Request:**  
           Sends the query to the Perplexity API using the `_send_request` method, which includes retry logic.
        
        3. **Response Parsing:**  
           Cleans and parses the returned JSON into a Python dictionary. If errors occur during the API call or JSON parsing,
           a dictionary is returned with an error message in 'response_summary' and default empty values for the other fields.
        
        Parameters
        ----------
        user_query : str
            The general query that you want to ask.
        custom_schema : dict or str, optional
            The desired JSON schema for the response. For example, a custom schema might be:
            
                {
                  "query": "<string, echo the user's query>",
                  "stocks": [
                    {
                      "symbol": "<string, ticker symbol>",
                      "earnings_growth": "<float, earnings growth percentage>",
                      "analyst_rating": "<float, average analyst rating from 1 to 5>",
                      "price_target": "<float, consensus price target in USD>"
                    }
                  ],
                  "summary": "<string, overall summary of findings>"
                }
            
            If no custom schema is provided, the default schema (shown above) is used.
        model : str, optional
            The model to use for the query. Supported models include "sonar", "sonar-pro", "sonar-reasoning", etc.
            The default model is "sonar".
        max_tokens : int, optional
            The maximum number of tokens to generate (default is 35000).
        stream : bool, optional
            Whether to enable streaming for longer responses (default is None, which enables streaming for large responses).
        
        Returns
        -------
        dict
            A dictionary containing the API's response following the specified JSON schema.
            In case of an error, returns a dictionary with an error message in 'response_summary'
            and empty values for the other keys.
        
        Raises
        ------
        Exception
            Propagates exceptions if the API call fails after the specified number of retries.
        
        Examples
        --------
        Using the default schema:
        
        >>> result = helper.execute_general_query("What factors are currently driving stock market volatility?")
        >>> print(result)
        {
          "query": "What factors are currently driving stock market volatility?",
          "response_summary": "Market volatility is influenced by economic indicators, regulatory changes, and shifts in investor sentiment.",
          "detailed_response": "Additional factors include earnings reports, interest rate adjustments, and geopolitical tensions.",
          "symbols": []
        }
        
        Using a custom schema:
        
        >>> custom_schema = {
        ...     "query": "<string, echo the user's query>",
        ...     "stocks": [
        ...         {
        ...             "symbol": "<string, ticker symbol>",
        ...             "earnings_growth": "<float, earnings growth percentage>",
        ...             "analyst_rating": "<float, average analyst rating from 1 to 5>",
        ...             "price_target": "<float, consensus price target in USD>"
        ...         }
        ...     ],
        ...     "summary": "<string, overall summary of findings>"
        ... }
        >>> result = helper.execute_general_query("Give me a list of current quantum computing stocks with their earnings growth, analyst ratings, and consensus price targets.", custom_schema, model="sonar-pro")
        >>> print(result)
        {
          "query": "Give me a list of current quantum computing stocks with their earnings growth, analyst ratings, and consensus price targets.",
          "stocks": [
            {
              "symbol": "QUBT",
              "earnings_growth": 200.0,
              "analyst_rating": 5.0,
              "price_target": 8.50
            },
            {
              "symbol": "QBTS",
              "earnings_growth": 64.0,
              "analyst_rating": 4.8,
              "price_target": 44.80
            },
            {
              "symbol": "IONQ",
              "earnings_growth": 100.0,
              "analyst_rating": 4.6,
              "price_target": 15.0
            },
            {
              "symbol": "RGTI",
              "earnings_growth": null,
              "analyst_rating": 5.0,
              "price_target": null
            }
          ],
          "summary": "The quantum computing sector shows diverse performance with some stocks exhibiting high earnings growth and bullish analyst ratings, while others remain more speculative."
        }
        """
        system_msg = self._build_general_prompt(user_query, custom_schema)
        schema = None
        if custom_schema is None:
            schema = DEFAULT_GENERAL_JSON_SCHEMA
        try:
            assistant_text = self._send_request(system_msg, user_query, model=model, max_tokens=max_tokens, stream=stream, schema=schema)
        except Exception as e:
            return {
                "query": user_query,
                "response_summary": f"Error calling Perplexity API: {str(e)}",
                "detailed_response": "",
                "symbols": []
            }
        
        # Clean the raw response to remove markdown formatting and any extraneous text
        cleaned_text = self._clean_response(assistant_text)
        try:
            data = json.loads(cleaned_text)
        except json.JSONDecodeError as e:
            logging.error(f"JSON decoding failed. Raw response: {cleaned_text}")
            return {
                "query": user_query,
                "response_summary": f"Error: LLM output was not valid JSON. {str(e)}",
                "detailed_response": "",
                "symbols": []
            }
        
        return data

    # --------------------------------------------------------------------------
    # Internal Post-Processing for Financial News Data
    # --------------------------------------------------------------------------
    def _post_process_data(self, data: dict) -> None:
        """
        Processes the financial news response data to ensure numeric fields have the correct type.
        
        Converts the following fields to integers:
          - confidence, sentiment_score, popularity_metric, magnitude, and volume_of_messages (if present)
        Also converts price target values to floats if provided.
        
        Parameters
        ----------
        data : dict
            The JSON response dictionary to post-process.
        """
        items = data.get("items", [])
        for item in items:
            for int_field in ("confidence", "sentiment_score", "popularity_metric", "magnitude"):
                if int_field in item:
                    try:
                        item[int_field] = int(item[int_field])
                    except (ValueError, TypeError):
                        item[int_field] = 0

            if "volume_of_messages" in item:
                try:
                    item["volume_of_messages"] = int(item["volume_of_messages"])
                except (ValueError, TypeError):
                    item["volume_of_messages"] = 0

            if "price_targets" in item and isinstance(item["price_targets"], dict):
                for float_field in ("low", "high", "average"):
                    if float_field in item["price_targets"]:
                        try:
                            item["price_targets"][float_field] = float(item["price_targets"][float_field])
                        except (ValueError, TypeError):
                            item["price_targets"][float_field] = None

# ------------------------------------------------------------------------------
# Example usage in a standalone script:
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import os
    import dotenv

    dotenv.load_dotenv()
    perplexity_api_key = os.getenv("PERPLEXITY_API_KEY")
    if not perplexity_api_key or perplexity_api_key == "YOUR_PERPLEXITY_API_KEY":
        print("WARNING: No valid PERPLEXITY_API_KEY found in environment!")
        perplexity_api_key = "YOUR_PERPLEXITY_API_KEY"  # fallback

    helper = PerplexityHelper(api_key=perplexity_api_key)

    # Example financial news query (focused on earnings and analyst recommendations)
    financial_query = "What are the latest earnings reports that came out today, and what price target recommendations are being given by leading analysts?"
    financial_result = helper.execute_financial_news_query(financial_query)
    print("Financial News Query Result:")
    print(json.dumps(financial_result, indent=2))

    # Example general query with default schema (trading-related)
    general_query = "What are some stocks that are very volatilitile right now?"
    general_result_default = helper.execute_general_query(general_query)
    print("\nGeneral Query Result (Default Schema):")
    print(json.dumps(general_result_default, indent=2))

    # Example general query with a custom JSON schema (trading-focused example)
    custom_schema = {
        "query": "<string, echo the user's query>",
        "stocks": [
            {
                "symbol": "<string, ticker symbol>",
                "earnings_growth": "<float, earnings growth percentage>",
                "analyst_rating": "<float, average analyst rating from 1 to 5>",
                "price_target": "<float, consensus price target in USD>"
            }
        ],
        "summary": "<string, overall summary of findings>"
    }
    general_result_custom = helper.execute_general_query("Give me a list of current quantum computing stocks with their earnings growth, analyst ratings, and consensus price targets.", custom_schema, model="sonar-pro")
    print("\nGeneral Query Result (Custom Schema):")
    print(json.dumps(general_result_custom, indent=2))