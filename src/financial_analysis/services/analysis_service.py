"""
LLM Multi-Agent Analysis Service
Uses Google Gemini API to perform fundamental analysis with three perspectives:
- Optimistic Agent (Bullish)
- Pessimistic Agent (Bearish)
- Neutral Analyst (Balanced/Judge)
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


class MultiAgentAnalysisService:
    """
    Service for using multiple LLM agents to analyze company news from different perspectives.
    Three agents provide bullish, bearish, and neutral fundamental analysis.
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
                    "temperature": 0.7,
                    "max_output_tokens": 2048,
                    "response_mime_type": "text/plain",
                },
            )
            self.logger.info(f"[OK] Multi-Agent Analysis Service configured with model: {DEFAULT_ANALYSIS_MODEL}")
        except Exception as e:
            self.logger.error(f"Error configuring Gemini: {e}")
            self.model = None

    def is_configured(self) -> bool:
        """Check if the service was successfully configured"""
        return self.model is not None

    def analyze_company_fundamentals(self, company_name: str, ticker: str, all_news_text: str, language: str = "English") -> Optional[str]:
        """
        Analyzes a large block of news text from three perspectives and returns a COMBINED string.

        Args:
            company_name: The name of the company.
            ticker: The stock ticker.
            all_news_text: A single large string containing all news fragments.
            language: The desired output language.

        Returns:
            A single string containing the combined analyses (Optimistic, Pessimistic, Neutral).
        """
        if not self.is_configured():
            self.logger.error("Cannot analyze: Service is not configured.")
            return None

        try:
            # Truncate text if it's extremely large to avoid token limits
            max_chars = 500_000
            if len(all_news_text) > max_chars:
                logging.warning(f"Truncating news text from {len(all_news_text)} to {max_chars} chars.")
                all_news_text = all_news_text[:max_chars]

            # 1. Call Optimistic Agent (Bullish)
            optimistic_prompt = self._create_optimistic_prompt(company_name, ticker, all_news_text, language)
            optimistic_result = self._call_agent("Optimistic Agent", optimistic_prompt) or "Analysis failed."

            # 2. Call Pessimistic Agent (Bearish)
            pessimistic_prompt = self._create_pessimistic_prompt(company_name, ticker, all_news_text, language)
            pessimistic_result = self._call_agent("Pessimistic Agent", pessimistic_prompt) or "Analysis failed."

            # 3. Call Neutral Analyst (Judge)
            neutral_prompt = self._create_neutral_prompt(
                company_name,
                ticker,
                all_news_text,
                language,
                optimistic_view=optimistic_result,
                pessimistic_view=pessimistic_result
            )
            neutral_result = self._call_agent("Neutral Analyst", neutral_prompt) or "Analysis failed."

            # 4. Combine results into a single formatted string for the GUI
            final_output = (
                f"# Multi-Agent Fundamental Analysis: {company_name} ({ticker})\n\n"
                f"{optimistic_result}\n\n"
                f"---\n\n"
                f"{pessimistic_result}\n\n"
                f"---\n\n"
                f"{neutral_result}"
            )

            return final_output

        except Exception as e:
            self.logger.error(f"Error analyzing fundamentals for {ticker}: {e}", exc_info=True)
            return None

    def _call_agent(self, agent_name: str, prompt: str, max_retries: int = 2) -> Optional[str]:
        """
        Calls the Gemini API for an agent with rate limiting and retries.
        """
        for attempt in range(max_retries + 1):
            try:
                self.logger.info(f"[THINKING] {agent_name} is thinking...")
                self.rate_limiter.wait_if_needed()
                time.sleep(0.2)

                response = self.model.generate_content(prompt)

                if response and response.text:
                    self.logger.info(f"[DONE] {agent_name} responded.")
                    return response.text.strip()
                else:
                    self.logger.warning(f"Empty response from {agent_name} (attempt {attempt + 1})")
                    if attempt < max_retries:
                        time.sleep(1 * (attempt + 1))
                        continue
                    return None

            except Exception as e:
                error_msg = str(e).lower()
                if 'rate' in error_msg or 'quota' in error_msg or '429' in error_msg:
                    wait_time = 5 * (attempt + 1)
                    if attempt < max_retries:
                        self.logger.warning(f"Rate limit hit, waiting {wait_time}s (retry {attempt + 1})")
                        time.sleep(wait_time)
                        continue

                self.logger.error(f"{agent_name} API error on attempt {attempt + 1}: {e}")
                if attempt >= max_retries:
                    return None
                time.sleep(2 * (attempt + 1))
        return None

    def _create_optimistic_prompt(self, company_name: str, ticker: str, news_text: str, language: str) -> str:
        """Creates the prompt for the optimistic/bullish agent."""

        if language.lower().startswith("portuguese"):
            lang_title = "Perspectiva Otimista - Oportunidades de Crescimento e Fortalezas"
            lang_intro = "Sua tarefa é identificar e enfatizar APENAS os aspectos POSITIVOS"
            lang_focus = "Forneça sua análise otimista aqui, focando em:"
        else:
            lang_title = "Bullish Perspective - Growth Opportunities & Strengths"
            lang_intro = "Your task is to identify and emphasize ONLY the POSITIVE aspects"
            lang_focus = "Provide your optimistic analysis here, focusing on:"

        prompt = f"""
You are a highly experienced BULLISH investment analyst with an extremely positive outlook on long-term growth.
{lang_intro} of the following news about a company.

Company: {company_name}
Ticker: {ticker}

Task:
1. Review the news articles below.
2. Highlight the BEST aspects: growth opportunities, competitive advantages, market expansion, innovation, strong management, positive trends.
3. Explain why these factors could drive LONG-TERM value creation.
4. Be optimistic but factual - cite specific events from the news.
5. Write your response in {language}.
6. DO NOT use emojis.
7. DO NOT start with greetings like "Sure!", "Here is the analysis". Start directly with the markdown header.

---

## {lang_title}

{lang_focus}
- Revenue and profit growth potential
- New market opportunities
- Competitive moats and advantages
- Innovation and R&D
- Management quality
- Positive market trends

---

News Articles:
{news_text}
"""
        return prompt

    def _create_pessimistic_prompt(self, company_name: str, ticker: str, news_text: str, language: str) -> str:
        """Creates the prompt for the pessimistic/bearish agent."""

        if language.lower().startswith("portuguese"):
            lang_title = "Perspectiva Pessimista - Riscos e Fraquezas"
            lang_intro = "Sua tarefa é identificar e enfatizar APENAS os RISCOS e PREOCUPAÇÕES"
            lang_focus = "Forneça sua análise pessimista aqui, focando em:"
        else:
            lang_title = "Bearish Perspective - Risks & Weaknesses"
            lang_intro = "Your task is to identify and emphasize ONLY the RISKS and CONCERNS"
            lang_focus = "Provide your pessimistic analysis here, focusing on:"

        prompt = f"""
You are a highly experienced BEARISH investment analyst with a cautious, critical outlook.
{lang_intro} in the following news about a company.

Company: {company_name}
Ticker: {ticker}

Task:
1. Review the news articles below.
2. Highlight the RISKS and CONCERNS: declining markets, competition, management issues, regulatory challenges, operational problems, negative trends.
3. Explain why these factors could threaten LONG-TERM value.
4. Be critical but factual - cite specific events from the news.
5. Write your response in {language}.
6. DO NOT use emojis.
7. DO NOT start with greetings. Start directly with the markdown header.

---

## {lang_title}

{lang_focus}
- Declining revenues or margins
- Increasing competition
- Regulatory or legal risks
- Management concerns
- Market headwinds
- Operational challenges

---

News Articles:
{news_text}
"""
        return prompt

    def _create_neutral_prompt(self, company_name: str, ticker: str, news_text: str, language: str, optimistic_view: str, pessimistic_view: str) -> str:
        """Creates the prompt for the neutral analyst (Fundamental Analysis & Key Events)."""

        if language.lower().startswith("portuguese"):
            lang_title_analysis = "Analise Fundamentalista"
            lang_title_summary = "Resumo dos Eventos Principais"
            lang_yes = "SIM, mudancas fundamentais significativas foram detectadas."
            lang_no = "NAO, mudancas fundamentais significativas NAO foram detectadas."
            lang_no_example = (
                "As noticias se concentraram principalmente em reacoes de mercado de curto prazo e "
                "classificacoes de analistas, que nao alteram a estrategia de negocios de longo prazo "
                "ou a posicao competitiva da empresa."
            )
        else:
            lang_title_analysis = "Fundamental Analysis"
            lang_title_summary = "Key Event Summary"
            lang_yes = "YES, significant fundamental changes were detected."
            lang_no = "NO, significant fundamental changes were NOT detected."
            lang_no_example = (
                "The news primarily focused on short-term market reactions and analyst ratings, "
                "which do not alter the company's long-term business strategy or competitive position."
            )

        prompt = f"""
You are an expert-level, long-term, fundamental "buy-and-hold" financial analyst acting as a JUDGE.
Your task is to review the arguments from a Bullish Agent (Optimistic) and a Bearish Agent (Pessimistic), along with the raw news, to determine if the investment thesis has broken.

Company: {company_name}
Ticker: {ticker}

--- AGENT ARGUMENTS ---

[OPTIMISTIC/BULLISH VIEW]
{optimistic_view}

[PESSIMISTIC/BEARISH VIEW]
{pessimistic_view}

--- INSTRUCTIONS ---

1. Weigh the arguments above against the raw news provided below.
2. Your analysis MUST focus *only* on events relevant to a long-term (5-10 year) fundamental investor.
3. IGNORE short-term price fluctuations, daily market volatility, analyst "buy/sell" ratings, and minor technical noise.
4. Provide your response in two distinct sections: ## {lang_title_analysis} and ## {lang_title_summary}.
5. Your entire response MUST be written in the following language: {language}
6. Do not include any other text, greetings, or pleasantries.
7. DO NOT use emojis.

---

## {lang_title_analysis}

In this section, explicitly state whether you detect any **Fundamental Thesis Breaks** or structural changes that would justify a re-evaluation of the investment.

You MUST answer "{lang_yes}" if ANY of the following "Structural Changes" are present in the news or highlighted by the agents:

1. **Thesis Break & Competitive Advantage:** Sharp structural revenue slowdown, margin collapse, loss of patents/moat.
2. **Leadership & Governance:** Replacement of founders/executives (CEO/CFO changes), fraud, accounting irregularities, sudden auditor changes.
3. **Capital Allocation & Balance Sheet:** Value-destroying acquisitions, reckless leverage, liquidity crunches, **REPEATED DILUTIONS (Equity Offerings/Follow-ons)**, dividend cuts due to distress.
4. **Competitive & Business Model:** New superior competitors, technological disruption, loss of pricing power, rising customer churn, declining ROIC.
5. **Strategy & Mission:** Strategic drift (pivoting to low-return areas), founder leaving, strategy becoming defensive vs innovative.
6. **Regulatory & External:** Major regulatory bans, antitrust actions, massive environmental liabilities (ESG risks).

**CRITICAL:** Events like **Stock Offerings (Follow-ons)**, **Management Changes (C-Level)**, and **Mergers/Acquisitions** are ALWAYS considered fundamental changes in this context.

Examples of NON-FUNDAMENTAL noise (Answer NO):
- "Stock fell 5% on profit-taking"
- "Analyst reiterates 'neutral' rating"
- "Market is down"
- "Minor quarterly earnings beat/miss" without structural reasons.

How to answer:
* If YES: Start with "{lang_yes}" Then, explain which specific structural change occurred (e.g., "Dilution risk via Follow-on offering..." or "Management change...").
* If NO: Start with "{lang_no}" Then, briefly explain what the news was about and why it does not constitute a fundamental change (e.g., "{lang_no_example}").

## {lang_title_summary}

In this section, provide a concise, neutral, bullet-point summary of the most important factual events reported in the news.
- Focus on verifiable facts (e.g., "Company reported 10% profit growth," "CEO announced a new factory in Manaus").
- Omit repetitive information.
- Keep it to a maximum of 5-7 key bullet points.

--- NEWS ---
{news_text}
"""
        return prompt


# Global instance
_analysis_service = None

def get_analysis_service() -> MultiAgentAnalysisService:
    """Get the global MultiAgentAnalysisService instance"""
    global _analysis_service
    if _analysis_service is None:
        _analysis_service = MultiAgentAnalysisService()
    return _analysis_service