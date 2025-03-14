import json
import time
import logging
import re
from openai import OpenAI

# Configure basic logging
logging.basicConfig(level=logging.INFO)

class GrokHelper:
    """
    A helper for querying the Grok/xAI API via an OpenAI-compatible client.
    
    This helper supports general queries only. You may supply a custom JSON schema 
    (as a dict or string) that thoroughly describes the expected JSON response.
    If no custom schema is provided, a default schema is used.
    """
    
    def __init__(self, api_key: str):
        """
        Initializes the GrokHelper with your API key and creates an OpenAI-compatible client.
        
        Parameters
        ----------
        api_key : str
            Your Grok/xAI API key. If not provided, the helper will attempt to read it from 
            the environment variables 'GROK_API_KEY' or 'XAI_API_KEY'.
        
        Raises
        ------
        ValueError
            If no API key is provided or found.
        """
        if not api_key:
            import os
            api_key = os.getenv("GROK_API_KEY") or os.getenv("XAI_API_KEY")
            if not api_key:
                raise ValueError("API key is required for GrokHelper. Set it as GROK_API_KEY or XAI_API_KEY in your environment or pass it directly.")
        
        self.api_key = api_key
        self.client = OpenAI(
            api_key=self.api_key,
            base_url="https://api.x.ai/v1",
        )
    
    def _clean_response(self, response_text: str) -> str:
        """
        Cleans the raw API response by removing markdown code fences and any extraneous text 
        preceding the first '{', so that only a valid JSON object remains.
        
        Parameters
        ----------
        response_text : str
            The raw response text from the API.
        
        Returns
        -------
        str
            The cleaned JSON string.
        """
        response_text = response_text.strip()
        # Remove markdown code fences if present
        if response_text.startswith("```"):
            lines = response_text.splitlines()
            filtered = [line for line in lines if not re.match(r"^```", line.strip())]
            response_text = "\n".join(filtered).strip()
        # Remove any text before the first '{'
        first_brace = response_text.find("{")
        if first_brace != -1:
            response_text = response_text[first_brace:]
        return response_text

    def _build_general_prompt(self, user_query: str, custom_schema=None) -> str:
        """
        Constructs a system prompt for general queries.
        
        You can provide a custom JSON schema as either a Python dictionary or a string.
        It is recommended that the schema thoroughly describes the expected JSON output,
        including detailed explanations for each field.
        
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
                  "founding_year": "<integer, year the company was founded>",
                  "sector": "<string, the primary sector of the company>"
                }
              ],
              "summary": "<string, overall summary of findings>"
            }
        
        Instruct the model to output only the JSON object with no extra text or markdown.
        
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
You are a knowledgeable assistant with access to real-time information via Grok/xAI.
Answer the user's query accurately and concisely, avoiding any extra explanation.
Return only the JSON object (with no markdown formatting or extra text) following this schema exactly.
Include detailed descriptions for each field so that the expected output is unambiguous.

The JSON schema is as follows:

{schema_str}

Instructions:
1) Output MUST be valid JSON (no extra text or markdown).
2) Do not include any preamble or code fences.
3) The 'query' field must exactly echo the user's query.

Now, the user's query is:
\"{user_query}\"

Return only valid JSON following the schema.
"""
        return system_prompt

    def _send_request(self, system_msg: str, user_query: str, model: str = "grok-2-latest", temperature: int = 0, retries: int = 3) -> str:
        """
        Sends a request to the Grok/xAI API using the provided system message and user query.
        Implements a retry loop to mitigate transient failures.
        Additional parameters like 'max_tokens' and 'top_p' are included to encourage a complete output.
        
        Parameters
        ----------
        system_msg : str
            The system message (prompt) to send.
        user_query : str
            The user's query.
        model : str, optional
            The model to use (default is "grok-2-latest").
        temperature : int, optional
            The temperature setting (default is 0).
        retries : int, optional
            Number of retry attempts (default is 3).
        
        Returns
        -------
        str
            The content of the API response.
        
        Raises
        ------
        Exception
            Propagates the last exception if all retries fail.
        """
        for attempt in range(1, retries + 1):
            try:
                completion = self.client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": user_query}
                    ],
                    temperature=temperature,
                    max_tokens=500,
                    top_p=0.9,
                    stream=False
                )
                if (not completion.choices or
                    not hasattr(completion.choices[0], "message") or
                    not hasattr(completion.choices[0].message, "content")):
                    raise ValueError("Invalid response structure from API.")
                response_text = completion.choices[0].message.content
                if not response_text.strip():
                    raise ValueError("Received empty response from API.")
                return response_text
            except Exception as e:
                logging.error(f"Attempt {attempt} failed: {e}")
                if attempt == retries:
                    logging.error(f"Final attempt failed. System prompt was: {system_msg}")
                    raise e
                time.sleep(1)
        raise RuntimeError("Failed to get a valid response after retries.")

    def execute_general_query(self, user_query: str, custom_schema=None, model: str = "grok-2-latest") -> dict:
        """
        Executes a general query using the Grok/xAI API.
        
        This method performs the following steps:
        
        1. **Prompt Construction:**  
           Builds a system prompt using the `_build_general_prompt` method. You may supply a custom JSON schema 
           (as a dict or string) that thoroughly describes the expected output. If no custom schema is provided, 
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
                     "founding_year": "<integer, year founded>",
                     "sector": "<string, primary sector of the company>"
                   }
                 ],
                 "summary": "<string, overall summary of findings>"
               }
        
        2. **API Request:**  
           Sends the query to the Grok/xAI API using the `_send_request` method, which includes retry logic.
        
        3. **Response Parsing:**  
           Cleans the returned text using `_clean_response()` and parses it into a Python dictionary.
           If JSON decoding fails, logs the raw cleaned response and returns a dictionary with an error message in 
           'response_summary' and default empty values for the other keys.
        
        Parameters
        ----------
        user_query : str
            The general query that you want to ask.
        custom_schema : dict or str, optional
            The desired JSON schema for the response.
        model : str, optional
            The model to use for the query. Supported models include "grok-2-latest", "grok-pro", etc.
            The default model is "grok-2-latest".
        
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
        
        >>> result = helper.execute_general_query("List some of the oldest technology stocks with their ticker symbols and founding years.")
        >>> print(result)
        {
          "query": "List some of the oldest technology stocks with their ticker symbols and founding years.",
          "response_summary": "Some of the oldest tech stocks include IBM, AT&T, and General Electric, with founding years 1911, 1885, and 1892 respectively.",
          "detailed_response": "",
          "symbols": []
        }
        
        Using a custom schema:
        
        >>> custom_schema = {
        ...     "query": "<string, echo the user's query>",
        ...     "stocks": [
        ...         {
        ...             "symbol": "<string, ticker symbol>",
        ...             "founding_year": "<integer, year founded>",
        ...             "sector": "<string, primary sector of the company>"
        ...         }
        ...     ],
        ...     "summary": "<string, overall summary of findings>"
        ... }
        >>> result = helper.execute_general_query("List some well-known leveraged ETFs that track the technology sector, including their ticker symbols and leverage factors.", custom_schema, model="grok-2-latest")
        >>> print(result)
        {
          "query": "List some well-known leveraged ETFs that track the technology sector, including their ticker symbols and leverage factors.",
          "stocks": [
            {
              "symbol": "TECL",
              "founding_year": 2008,
              "sector": "Technology"
            },
            {
              "symbol": "TQQQ",
              "founding_year": 2008,
              "sector": "Technology"
            }
          ],
          "summary": "Leveraged ETFs such as TECL and TQQQ have been popular for tracking the technology sector with significant leverage."
        }
        """
        system_msg = self._build_general_prompt(user_query, custom_schema)
        try:
            assistant_text = self._send_request(system_msg, user_query, model=model)
        except Exception as e:
            return {
                "query": user_query,
                "response_summary": f"Error calling Grok/xAI API: {str(e)}",
                "detailed_response": "",
                "symbols": []
            }
        
        cleaned_text = self._clean_response(assistant_text)
        try:
            data = json.loads(cleaned_text)
        except json.JSONDecodeError as e:
            logging.error(f"JSON decoding failed. Raw response: {cleaned_text}")
            return {
                "query": user_query,
                "response_summary": f"Error: Output was not valid JSON. {str(e)}",
                "detailed_response": "",
                "symbols": []
            }
        
        return data

# ------------------------------------------------------------------------------
# Example usage in a standalone script:
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import os
    import dotenv

    dotenv.load_dotenv()
    grok_api_key = os.getenv("GROK_API_KEY") or os.getenv("XAI_API_KEY")
    if not grok_api_key or grok_api_key == "YOUR_GROK_API_KEY":
        print("WARNING: No valid GROK_API_KEY found in environment!")
        grok_api_key = "YOUR_GROK_API_KEY"  # fallback

    helper = GrokHelper(api_key=grok_api_key)

    # Example general query using the default schema (trading-related)
    general_query = "List some of the oldest technology stocks with their ticker symbols and founding years."
    general_result_default = helper.execute_general_query(general_query)
    print("\nGeneral Query Result (Default Schema):")
    print(json.dumps(general_result_default, indent=2))

    # Example general query using a custom schema (trading-focused example)
    custom_schema = {
        "query": "<string, echo the user's query>",
        "stocks": [
            {
                "symbol": "<string, ticker symbol>",
                "founding_year": "<integer, year founded>",
                "sector": "<string, primary sector of the company>"
            }
        ],
        "summary": "<string, overall summary of findings>"
    }
    general_result_custom = helper.execute_general_query("List some well-known leveraged ETFs that track the technology sector, including their ticker symbols and leverage factors.", custom_schema, model="grok-2-latest")
    print("\nGeneral Query Result (Custom Schema):")
    print(json.dumps(general_result_custom, indent=2))