"""
Tests unitaires et d'intégration pour le scraper Rotten Tomatoes.

Structure des tests :
  - TestCleanText      : tests unitaires de la fonction clean_text()
  - TestGetInfoBs4     : tests de get_info_bs4() avec des réponses HTTP mockées
  - TestResumptionLogic: tests de la logique de reprise (fichiers existants)
  - TestIntegration    : tests de bout-en-bout sur un HTML représentatif
"""

import csv
import json
from unittest.mock import MagicMock, Mock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Évite d'importer Config et requests directement si l'env n'est pas installé.
# On mock Config au niveau du module scraped.
FAKE_RT_COLUMNS = [
    "url_rotten",
    "title",
    "year",
    "rt_tomatometer",
    "rt_audience_score",
    "rt_critics_consensus",
]


def _make_fake_config():
    cfg = MagicMock()
    cfg.RT_COLUMNS = FAKE_RT_COLUMNS
    cfg.INPUT_CSV = "input.csv"
    cfg.OUTPUT_CSV = "output.csv"
    return cfg


# ---------------------------------------------------------------------------
# Fixture : patch Config avant tout import du module cible
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def patch_config():
    """Injecte un Config factice pour tous les tests."""
    with patch.dict("sys.modules", {"config": MagicMock(Config=_make_fake_config())}):
        yield


# ---------------------------------------------------------------------------
# Import tardif du module (après le patch de Config)
# ---------------------------------------------------------------------------


@pytest.fixture()
def scraper():
    """Importe le module scraper après que Config est mocké."""
    import importlib
    import sys

    # Supprime le cache pour forcer un rechargement propre
    sys.modules.pop("crawler", None)
    mod = importlib.import_module("crawler")
    return mod


# ============================================================================
# 1. Tests de clean_text
# ============================================================================


class TestCleanText:
    """Tests unitaires de la fonction clean_text()."""

    @pytest.fixture(autouse=True)
    def _import(self, scraper):
        self.clean_text = scraper.clean_text

    # --- Cas nominaux ---

    def test_removes_em_tags(self):
        assert self.clean_text("Hello <em>world</em>!") == "Hello world !"

    def test_replaces_closing_em_with_space(self):
        """</em> doit insérer un espace pour éviter la fusion de mots."""
        result = self.clean_text("foo</em>bar")
        assert "foo" in result and "bar" in result
        assert "foobar" not in result

    def test_flattens_newlines(self):
        result = self.clean_text("line1\nline2\r\nline3")
        assert "\n" not in result
        assert "\r" not in result

    def test_collapses_multiple_spaces(self):
        assert self.clean_text("a   b    c") == "a b c"

    def test_strips_leading_trailing_whitespace(self):
        assert self.clean_text("  hello  ") == "hello"

    def test_combined_html_and_whitespace(self):
        raw = "  <em>Great</em>  film\n\r ever.  "
        result = self.clean_text(raw)
        assert result == "Great film ever."

    # --- Cas limites ---

    def test_empty_string_returns_empty(self):
        assert self.clean_text("") == ""

    def test_none_returns_empty(self):
        assert self.clean_text(None) == ""

    def test_only_tags_returns_empty_or_space(self):
        result = self.clean_text("<em></em>")
        assert result == ""

    def test_tabs_are_collapsed(self):
        assert self.clean_text("a\t\t\tb") == "a b"

    def test_plain_text_unchanged(self):
        assert self.clean_text("Simple text.") == "Simple text."


# ============================================================================
# 2. Tests de get_info_bs4
# ============================================================================


def _build_html(
    title="Inception",
    year="2010",
    critics_score=87,
    audience_score=91,
    consensus="A mind-bending thriller.",
):
    """Génère un HTML minimal qui imite la structure de Rotten Tomatoes."""
    scorecard = json.dumps(
        {
            "criticsScore": {"score": critics_score},
            "audienceScore": {"score": audience_score},
        }
    )
    return f"""
    <html><body>
      <script data-json="mediaScorecard">{scorecard}</script>
      <rt-text slot="title">{title}</rt-text>
      <rt-text slot="metadata-prop">{year}</rt-text>
      <div id="critics-consensus"><p>{consensus}</p></div>
    </body></html>
    """


def _mock_response(html, status_code=200):
    resp = Mock()
    resp.status_code = status_code
    resp.text = html
    return resp


class TestGetInfoBs4:
    """Tests de get_info_bs4 avec des réponses HTTP mockées."""

    @pytest.fixture(autouse=True)
    def _setup(self, scraper):
        self.get_info = scraper.get_info_bs4
        self.session = MagicMock()

    def test_extracts_title(self):
        self.session.get.return_value = _mock_response(_build_html(title="Inception"))
        row = self.get_info(self.session, "https://rt.com/inception")
        assert row["title"] == "Inception"

    def test_extracts_year(self):
        self.session.get.return_value = _mock_response(_build_html(year="2010"))
        row = self.get_info(self.session, "https://rt.com/inception")
        assert row["year"] == "2010"

    def test_extracts_tomatometer(self):
        self.session.get.return_value = _mock_response(_build_html(critics_score=87))
        row = self.get_info(self.session, "https://rt.com/inception")
        assert row["rt_tomatometer"] == 87

    def test_extracts_audience_score(self):
        self.session.get.return_value = _mock_response(_build_html(audience_score=91))
        row = self.get_info(self.session, "https://rt.com/inception")
        assert row["rt_audience_score"] == 91

    def test_extracts_critics_consensus(self):
        self.session.get.return_value = _mock_response(
            _build_html(consensus="A mind-bending thriller.")
        )
        row = self.get_info(self.session, "https://rt.com/inception")
        assert row["rt_critics_consensus"] == "A mind-bending thriller."

    def test_url_is_preserved(self):
        url = "https://rt.com/some_movie"
        self.session.get.return_value = _mock_response(_build_html())
        row = self.get_info(self.session, url)
        assert row["url_rotten"] == url

    def test_http_error_returns_empty_row(self):
        self.session.get.return_value = _mock_response("", status_code=404)
        row = self.get_info(self.session, "https://rt.com/notfound")
        assert row["title"] == ""
        assert row["rt_tomatometer"] == ""

    def test_network_exception_returns_empty_row(self):
        self.session.get.side_effect = Exception("Connection refused")
        row = self.get_info(self.session, "https://rt.com/error")
        assert row["title"] == ""
        assert row["rt_tomatometer"] == ""

    def test_missing_scorecard_script_leaves_scores_empty(self):
        html = "<html><body><rt-text slot='title'>Film</rt-text></body></html>"
        self.session.get.return_value = _mock_response(html)
        row = self.get_info(self.session, "https://rt.com/film")
        assert row["rt_tomatometer"] == ""
        assert row["rt_audience_score"] == ""

    def test_missing_consensus_leaves_field_empty(self):
        html = "<html><body></body></html>"
        self.session.get.return_value = _mock_response(html)
        row = self.get_info(self.session, "https://rt.com/film")
        assert row["rt_critics_consensus"] == ""

    def test_year_outside_valid_range_is_ignored(self):
        html = _build_html(year="1850")  # avant 1890 → ignoré
        self.session.get.return_value = _mock_response(html)
        row = self.get_info(self.session, "https://rt.com/film")
        assert row["year"] == ""

    def test_consensus_with_em_tags_is_cleaned(self):
        html = _build_html(consensus="A <em>stunning</em> film.")
        self.session.get.return_value = _mock_response(html)
        row = self.get_info(self.session, "https://rt.com/film")
        assert "<em>" not in row["rt_critics_consensus"]
        assert "stunning" in row["rt_critics_consensus"]

    def test_returns_all_expected_columns(self):
        self.session.get.return_value = _mock_response(_build_html())
        row = self.get_info(self.session, "https://rt.com/film")
        for col in FAKE_RT_COLUMNS:
            assert col in row, f"Colonne manquante : {col}"

    def test_timeout_is_set(self):
        """Vérifie que session.get est appelé avec un timeout."""
        self.session.get.return_value = _mock_response(_build_html())
        self.get_info(self.session, "https://rt.com/film")
        call_kwargs = self.session.get.call_args[1]
        assert "timeout" in call_kwargs


# ============================================================================
# 3. Tests de la logique de reprise (resumption)
# ============================================================================


class TestResumptionLogic:
    """
    Teste la détection des URLs déjà traitées via un fichier CSV existant.
    Ces tests simulent le comportement du bloc __main__ de manière isolée.
    """

    def _write_csv(self, path, rows):
        if not rows:
            return
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FAKE_RT_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

    def test_done_urls_are_skipped(self, scraper, tmp_path):
        """Les URLs présentes dans le fichier de sortie ne doivent pas être retraitées."""
        output_csv = tmp_path / "output.csv"
        already_done = "https://rt.com/done_movie"
        self._write_csv(
            output_csv,
            [
                {
                    "url_rotten": already_done,
                    **{k: "" for k in FAKE_RT_COLUMNS if k != "url_rotten"},
                }
            ],
        )

        import polars as pl

        done_df = pl.read_csv(str(output_csv))
        done_urls = set(done_df["url_rotten"].to_list())
        all_urls = [already_done, "https://rt.com/new_movie"]
        urls_to_process = [u for u in all_urls if u not in done_urls]

        assert already_done not in urls_to_process
        assert "https://rt.com/new_movie" in urls_to_process

    def test_empty_output_file_processes_all_urls(self):
        """Si le fichier de sortie est absent, toutes les URLs sont traitées."""
        all_urls = ["https://rt.com/a", "https://rt.com/b"]
        done_urls = set()
        urls_to_process = [u for u in all_urls if u not in done_urls]
        assert urls_to_process == all_urls

    def test_all_done_exits_early(self):
        """Si toutes les URLs sont traitées, la liste de traitement est vide."""
        all_urls = ["https://rt.com/a"]
        done_urls = {"https://rt.com/a"}
        urls_to_process = [u for u in all_urls if u not in done_urls]
        assert urls_to_process == []


# ============================================================================
# 4. Tests d'intégration
# ============================================================================


class TestIntegration:
    """
    Teste le pipeline complet : get_info_bs4 + écriture CSV.
    Utilise un fichier temporaire réel pour valider le flux de bout-en-bout.
    """

    def test_full_pipeline_writes_valid_csv(self, scraper, tmp_path):
        output = tmp_path / "output.csv"
        session = MagicMock()
        session.get.return_value = _mock_response(
            _build_html(
                title="Parasite",
                year="2019",
                critics_score=99,
                audience_score=90,
                consensus="A masterclass in tension.",
            )
        )

        result = scraper.get_info_bs4(session, "https://rt.com/parasite")

        with open(output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FAKE_RT_COLUMNS)
            writer.writeheader()
            writer.writerow(result)

        import polars as pl

        df = pl.read_csv(str(output))
        assert df.shape[0] == 1
        assert df["title"][0] == "Parasite"
        assert df["year"][0] == 2019
        assert df["rt_tomatometer"][0] == 99
        assert df["rt_critics_consensus"][0] == "A masterclass in tension."

    def test_multiple_urls_written_sequentially(self, scraper, tmp_path):
        output = tmp_path / "output.csv"
        session = MagicMock()
        movies = [
            ("Matrix", "1999", 87, 85, "Groundbreaking sci-fi."),
            ("Alien", "1979", 98, 94, "A timeless horror."),
        ]

        with open(output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FAKE_RT_COLUMNS)
            writer.writeheader()
            for title, year, cs, aus, cons in movies:
                session.get.return_value = _mock_response(
                    _build_html(title, year, cs, aus, cons)
                )
                row = scraper.get_info_bs4(session, f"https://rt.com/{title.lower()}")
                writer.writerow(row)

        import polars as pl

        df = pl.read_csv(str(output))
        assert df.shape[0] == 2
        assert set(df["title"].to_list()) == {"Matrix", "Alien"}
