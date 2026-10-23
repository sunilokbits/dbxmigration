"""The Accelerator Video modal plays the bundled MP4, which must stay small
enough to ship as an app source file."""

import os
import re
import unittest

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VIDEO = os.path.join(HERE, "static", "video", "insight_migration_studio_showcase.mp4")
POSTER = os.path.join(HERE, "static", "video", "insight_migration_studio_poster.jpg")


class TestAcceleratorVideo(unittest.TestCase):
    def test_video_and_poster_are_bundled_and_small(self):
        self.assertTrue(os.path.exists(VIDEO))
        self.assertTrue(os.path.exists(POSTER))
        self.assertLess(os.path.getsize(VIDEO), 10 * 1024 * 1024, "app source files must stay under 10 MB")

    def test_modal_plays_the_mp4_with_download(self):
        html = open(os.path.join(HERE, "templates", "index.html"), encoding="utf-8").read()
        modal = html[html.index("ACCELERATOR VIDEO MODAL"):html.index('<script src="/static/supplementary.js')]
        self.assertIn('<video id="accelVideo" controls', modal)
        self.assertIn('src="/static/video/insight_migration_studio_showcase.mp4"', modal)
        self.assertIn('download="Insight_Migration_Studio_Showcase.mp4"', modal)
        self.assertNotIn("vidVoiceToggle", modal)

    def test_player_script_has_chapters_and_no_speech_synthesis(self):
        js = open(os.path.join(HERE, "static", "supplementary.js"), encoding="utf-8").read()
        a = js.index("Accelerator Video — narrated MP4")
        block = js[a:js.index("Supplementary video module error", a)]
        self.assertEqual(len(re.findall(r"\{t:[\d.]+,\s*title:", block)), 15)
        self.assertNotIn("speechSynthesis", block)
        for fn in ("openAcceleratorVideo", "closeAcceleratorVideo", "seekToChapter"):
            self.assertIn(f"window.{fn}=", block)


if __name__ == "__main__":
    unittest.main()
