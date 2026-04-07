from __future__ import annotations

import unittest

from tests._app_bootstrap import bootstrap_app_package

bootstrap_app_package("library_tracker", modules=("services",))

from library_tracker.web.services import library_service  # noqa: E402


class TestLibraryKeywordRules(unittest.TestCase):
	def test_multi_word_english_alias_keeps_phrase_not_unigrams(self):
		terms = library_service._extract_keyword_terms("Liz and the Blue Bird")
		self.assertIn("liz and the blue bird", terms)
		self.assertNotIn("and", terms)
		self.assertNotIn("the", terms)
		self.assertNotIn("bird", terms)

	def test_mixed_cjk_and_english_alias_keeps_cjk_and_phrase(self):
		terms = library_service._extract_keyword_terms("周杰伦 Jay Chou")
		self.assertIn("周杰伦 jay chou", terms)
		self.assertIn("周杰伦", terms)
		self.assertIn("jay chou", terms)
		self.assertNotIn("jay", terms)
		self.assertNotIn("chou", terms)

	def test_keyword_score_normalizes_fullwidth_punctuation_variants(self):
		item = {
			"title": "Tchaikovsky：Violin Concerto ＆ Serenade melancolique",
			"author": "Itzhak Perlman",
			"nationality": "美国",
			"category": "古典",
			"channel": "Apple Music",
			"review": "常听的版本。",
			"publisher": "EMI",
			"url": "",
		}

		score = library_service.core._keyword_score(item, "Tchaikovsky: Violin Concerto")
		self.assertGreater(score, 0.0)


if __name__ == "__main__":
	unittest.main(verbosity=2)
