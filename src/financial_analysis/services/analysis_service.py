"""
LLM Analysis Service
Uses Google Gemini API to perform fundamental analysis and summarization
based on a collection of news articles.
"""

import logging
import os
import time
from typing import Optional
from pathlib import Path
from datetime import datetime

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / '.env'
    load_dotenv(dotenv_path=env_path)
except ImportError:
    pass

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

# Use a model capable of handling larger contexts and reasoning
DEFAULT_ANALYSIS_MODEL = "gemini-2.0-flash-lite"

class RateLimiter:
    """
    Rate limiter to prevent exceeding API limits.
    (Copied from llm_service.py)
    """
    def __init__(self, requests_per_minute: int = 14):
        self.requests_per_minute = requests_per_minute
        self.requests_made = 0
        self.window_start = datetime.now()
        self.logger = logging.getLogger(__name__)

    def wait_if_needed(self):
        now = datetime.now()
        elapsed = (now - self.window_start).total_seconds()
        if elapsed >= 60:
            self.requests_made = 0
            self.window_start = now
            elapsed = 0
        if self.requests_made >= self.requests_per_minute:
            wait_time = 60 - elapsed + 1
            if wait_time > 0:
                self.logger.info(f"Rate limit: waiting {wait_time:.1f}s")
                time.sleep(wait_time)
                self.requests_made = 0
                self.window_start = datetime.now()
        self.requests_made += 1


class AnalysisService:
    """
    Service for using an LLM to analyze company news for fundamental changes.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        self.api_key = api_key or os.getenv('GEMINI_API_KEY')
        self.rate_limiter = RateLimiter(requests_per_minute=14)
        self.model = None

        if not GEMINI_AVAILABLE:
            self.logger.error("google-generativeai package not installed. Run: pip install google-generativeai")
            return
        if not self.api_key:
            self.logger.warning("GEMINI_API_KEY not found in .env file. Get a key at: https://aistudio.google.com/app/apikey")
            return

        try:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel(
                model_name=DEFAULT_ANALYSIS_MODEL,
                generation_config={
                    "temperature": 0.2, # Low temp for factual summary
                    "max_output_tokens": 4096, # Allow for a long, detailed response
                    "response_mime_type": "text/plain",
                },
                # Add safety settings to be less restrictive if needed
                # safety_settings={
                #     'HATE_SPEECH': 'BLOCK_NONE',
                #     'HARASSMENT': 'BLOCK_NONE',
                #     'SEXUALLY_EXPLICIT': 'BLOCK_NONE',
                #     'DANGEROUS_CONTENT': 'BLOCK_NONE'
                # }
            )
            self.logger.info(f"✅ Gemini Analysis Service configured with model: {DEFAULT_ANALYSIS_MODEL}")
        except Exception as e:
            self.logger.error(f"Error configuring Gemini: {e}")
            self.model = None

    def is_configured(self) -> bool:
        """Check if the service was successfully configured"""
        return self.model is not None

    def analyze_company_fundamentals(self, company_name: str, ticker: str, all_news_text: str) -> Optional[str]:
        """
        Analyzes a large block of news text for fundamental changes and key events.

        Args:
            company_name: The name of the company.
            ticker: The stock ticker.
            all_news_text: A single large string containing all news fragments.

        Returns:
            A formatted string with the analysis, or None if it fails.
        """
        if not self.is_configured():
            self.logger.error("Cannot analyze: Service is not configured.")
            return None

        try:
            # Truncate text if it's extremely large (Gemini 1.5 has a large context, but let's be safe)
            # 1.5 Flash has 1M token context, so this is likely fine, but good practice.
            max_chars = 500_000
            if len(all_news_text) > max_chars:
                logging.warning(f"Truncating news text from {len(all_news_text)} to {max_chars} chars.")
                all_news_text = all_news_text[:max_chars]

            prompt = self._create_analysis_prompt(company_name, ticker, all_news_text)

            response = self._call_gemini_api(prompt)

            if response:
                return response
            else:
                self.logger.warning(f"No response from LLM for {ticker} analysis.")
                return None
        except Exception as e:
            self.logger.error(f"Error analyzing fundamentals for {ticker}: {e}", exc_info=True)
            return None

    def _create_analysis_prompt(self, company_name: str, ticker: str, news_text: str) -> str:
        """Creates the Master Control Prompt (MCP) for the analysis task."""

        prompt = f"""
You are an expert-level, long-term, fundamental "buy-and-hold" financial analyst.
Your task is to analyze a collection of news articles about a specific company and provide a concise summary for a long-term investor.

**Company:** {company_name}
**Ticker:** {ticker}

**Instructions:**
1.  Read all the provided news text below the "--- NEWS ---" separator. The news articles are concatenated and may contain duplicates.
2.  Your analysis MUST focus *only* on events relevant to a long-term (5-10 year) fundamental investor.
3.  **IGNORE** short-term price fluctuations, daily market volatility, analyst "buy/sell" ratings, and minor technical noise.
4.  Provide your response in two distinct sections: `## Fundamental Analysis` and `## Key Event Summary`.
5.  Do not include any other text, greetings, or pleasantries.

---

## Fundamental Analysis
In this section, explicitly state whether you detect any significant, long-term **fundamental changes** to the company's business model, competitive advantages (moat), management, or long-term outlook based *only* on this news.

* Examples of FUNDAMENTAL changes: Mergers & Acquisitions, new revolutionary product line, major change in regulation, new CEO with a new strategy, major factory destroyed, evidence of fraud.
* Examples of NON-FUNDAMENTAL noise: "Stock fell 5% on profit-taking," "Analyst reiterates 'neutral' rating," "Market is down."

Start this section with "YES" or "NO" (e.g., "YES, significant fundamental changes were detected.") and then explain *why* in 2-3 bullet points. If no, state "NO, only short-term noise and regular business operations were detected."

## Key Event Summary
In this section, provide a concise, neutral, bullet-point summary of the *most important* factual events reported in the news.
* Focus on verifiable facts (e.g., "Company reported 10% profit growth," "CEO announced a new factory in
    Manaus," "A new competitor product was launched").
* Omit repetitive information.
* Keep it to a maximum of 5-7 key bullet points.

--- NEWS ---
{news_text}
"""
        return prompt

    def _call_gemini_api(self, prompt: str, max_retries: int = 2) -> Optional[str]:
        """
        Calls the Gemini API with rate limiting and retries.
        (Adapted from llm_service.py)
        """
        for attempt in range(max_retries + 1):
            try:
                self.rate_limiter.wait_if_needed()
                time.sleep(0.2) # Small buffer

                self.logger.info("Generating content with Gemini...")
                response = self.model.generate_content(prompt)

                if response and response.text:
                    self.logger.info("...Gemini response received.")
                    return response.text.strip()
                else:
                    self.logger.warning(f"Empty response from Gemini (attempt {attempt + 1})")
                    if attempt < max_retries:
                        time.sleep(1 * (attempt + 1))
                        continue
                    return None

            except Exception as e:
                error_msg = str(e).lower()
                if 'rate' in error_msg or 'quota' in error_msg or '429' in error_msg:
                    wait_time = 5 * (attempt + 1) # Wait longer for analysis tasks
                    if attempt < max_retries:
                        self.logger.warning(f"Rate limit hit, waiting {wait_time}s (retry {attempt + 1})")
                        time.sleep(wait_time)
                        continue

                self.logger.error(f"Gemini API error on attempt {attempt + 1}: {e}")
                if attempt >= max_retries:
                    return None
                time.sleep(2 * (attempt + 1))
        return None

# Global instance
_analysis_service = None

def get_analysis_service() -> AnalysisService:
    """Get the global AnalysisService instance"""
    global _analysis_service
    if _analysis_service is None:
        _analysis_service = AnalysisService()
    return _analysis_service