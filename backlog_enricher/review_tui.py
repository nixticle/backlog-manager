from __future__ import annotations

import logging
import webbrowser
from dataclasses import dataclass
from typing import List

import orjson

from .config import Config
from .db import Database
from .match import GameRow, store_manual_match

LOG = logging.getLogger(__name__)

try:
    from textual import events
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.widgets import Footer, Header, Static
except ModuleNotFoundError:  # pragma: no cover - textual optional fallback
    App = None  # type: ignore


@dataclass(slots=True)
class ReviewCandidate:
    payload: dict


@dataclass(slots=True)
class ReviewItem:
    game: GameRow
    title: str
    platform: str | None
    year: int | None
    status: str | None
    rating: float | None
    candidates: list[ReviewCandidate]


def run_review(cfg: Config, db: Database, dry_run: bool = False) -> None:
    items = load_review_items(db)
    if not items:
        print("Review queue is empty.")
        return
    controller = ReviewController(cfg, db, items, dry_run)
    if App is None:
        LOG.warning("textual_not_available_fallback")
        controller.run_cli()
    else:
        ReviewApp.run(controller=controller)


def load_review_items(db: Database) -> list[ReviewItem]:
    rows = db.query(
        """
        SELECT g.id, g.title, g.title_norm, g.platform, g.platform_family, g.year,
               g.status, g.rating, q.candidates_json
        FROM review_queue q
        JOIN games g ON g.id = q.game_id
        ORDER BY q.created_at ASC
        """
    )
    items: list[ReviewItem] = []
    for row in rows:
        try:
            candidates_raw = orjson.loads(row["candidates_json"])
        except orjson.JSONDecodeError:
            LOG.error("invalid_queue_payload", extra={"game_id": row["id"]})
            continue
        game = GameRow(
            id=row["id"],
            title=row["title"],
            title_norm=row["title_norm"],
            platform_family=row["platform_family"],
            year=row["year"],
        )
        candidates = [ReviewCandidate(payload=candidate) for candidate in candidates_raw]
        items.append(
            ReviewItem(
                game=game,
                title=row["title"],
                platform=row["platform"],
                year=row["year"],
                status=row["status"],
                rating=row["rating"],
                candidates=candidates,
            )
        )
    return items


class ReviewController:
    def __init__(self, cfg: Config, db: Database, items: list[ReviewItem], dry_run: bool):
        self.cfg = cfg
        self.db = db
        self.items = items
        self.dry_run = dry_run
        self.index = 0

    @property
    def current(self) -> ReviewItem:
        return self.items[self.index]

    def next_item(self) -> None:
        if self.index < len(self.items) - 1:
            self.index += 1

    def previous_item(self) -> None:
        if self.index > 0:
            self.index -= 1

    def choose(self, idx: int) -> str:
        item = self.current
        if idx >= len(item.candidates):
            return "Invalid selection."
        candidate_payload = item.candidates[idx].payload
        if self.dry_run:
            return f"Dry-run: would match '{item.title}' to '{candidate_payload['title']}'."
        store_manual_match(self.db, item.game, candidate_payload)
        del self.items[self.index]
        if self.index >= len(self.items):
            self.index = max(0, len(self.items) - 1)
        return f"Matched '{item.title}' manually."

    def skip(self) -> str:
        self.next_item()
        if self.index >= len(self.items):
            return "End of review queue."
        return "Skipped."

    def run_cli(self) -> None:
        import sys

        while self.items:
            item = self.current
            print(f"\nGame: {item.title} ({item.year or 'unknown'}) [{item.platform or 'unknown'}]")
            print("Candidates:")
            for idx, candidate in enumerate(item.candidates, start=1):
                payload = candidate.payload
                print(
                    f" {idx}. {payload['title']} (score={payload['score']}, year={payload['year']}, platforms={payload['platforms']})"
                )
            choice = input("Select candidate [1-5], s skip, q quit: ").strip().lower()
            if choice == "q":
                print("Exiting review.")
                return
            if choice == "s":
                message = self.skip()
                print(message)
                continue
            if choice.isdigit():
                message = self.choose(int(choice) - 1)
                print(message)
                if not self.items:
                    print("Review queue cleared.")
                    return
            else:
                print("Invalid input.")
        print("Review queue cleared.")


if App is not None:

    class ReviewApp(App):
        CSS = """
        Screen {
            align: center middle;
        }
        #body {
            width: 90%;
            height: 90%;
        }
        """
        BINDINGS = [
            Binding(key="q", action="quit", description="Quit"),
            Binding(key="s", action="skip", description="Skip"),
            Binding(key="n", action="next", description="Next"),
            Binding(key="p", action="previous", description="Previous"),
            Binding(key="o", action="open", description="Open URL"),
            Binding(key="1", action="choose_1", description="Choose 1"),
            Binding(key="2", action="choose_2", description="Choose 2"),
            Binding(key="3", action="choose_3", description="Choose 3"),
            Binding(key="4", action="choose_4", description="Choose 4"),
            Binding(key="5", action="choose_5", description="Choose 5"),
        ]

        def __init__(self, controller: ReviewController):
            super().__init__()
            self.controller = controller
            self.body = Static(id="body")
            self.message = Static("")

        def compose(self) -> ComposeResult:  # type: ignore[override]
            yield Header()
            yield self.body
            yield self.message
            yield Footer()

        def on_mount(self) -> None:  # type: ignore[override]
            self._refresh()

        def action_next(self) -> None:
            self.controller.next_item()
            self._refresh()

        def action_previous(self) -> None:
            self.controller.previous_item()
            self._refresh()

        def action_skip(self) -> None:
            message = self.controller.skip()
            self._refresh(message)

        def action_open(self) -> None:
            item = self.controller.current
            if not item.candidates:
                return
            url = item.candidates[0].payload.get("source_url")
            if url:
                webbrowser.open(url)
                self._update_message(f"Opened {url}")

        def action_choose_1(self) -> None:
            self._handle_choose(0)

        def action_choose_2(self) -> None:
            self._handle_choose(1)

        def action_choose_3(self) -> None:
            self._handle_choose(2)

        def action_choose_4(self) -> None:
            self._handle_choose(3)

        def action_choose_5(self) -> None:
            self._handle_choose(4)

        def _handle_choose(self, index: int) -> None:
            if not self.controller.items:
                self.exit()
                return
            message = self.controller.choose(index)
            if not self.controller.items:
                self.exit(message=message)
            else:
                self._refresh(message)

        def _refresh(self, message: str | None = None) -> None:
            if not self.controller.items:
                self.body.update("Review queue cleared.")
                self.message.update(message or "")
                return
            item = self.controller.current
            lines: List[str] = []
            lines.append(f"[bold]Game:[/bold] {item.title}")
            details = []
            if item.year:
                details.append(str(item.year))
            if item.platform:
                details.append(item.platform)
            if item.status:
                details.append(item.status)
            if item.rating is not None:
                details.append(f"rating {item.rating}")
            if details:
                lines.append(" • ".join(details))
            lines.append("")
            lines.append("[bold]Candidates:[/bold]")
            for idx, candidate in enumerate(item.candidates, start=1):
                payload = candidate.payload
                lines.append(
                    f"{idx}. {payload['title']} (score={payload['score']}, year={payload['year']}, "
                    f"platforms={', '.join(payload['platforms'])})"
                )
                durations = []
                if payload.get("main"):
                    durations.append(f"Main {payload['main']}h")
                if payload.get("main_extra"):
                    durations.append(f"Main+Extra {payload['main_extra']}h")
                if payload.get("complete"):
                    durations.append(f"Complete {payload['complete']}h")
                if durations:
                    lines.append("   " + ", ".join(durations))
                if payload.get("source_url"):
                    lines.append(f"   URL: {payload['source_url']}")
            self.body.update("\n".join(lines))
            if message:
                self._update_message(message)

        def _update_message(self, message: str) -> None:
            self.message.update(message)

        def on_key(self, event: events.Key) -> None:  # type: ignore[override]
            if event.key == "escape":
                self.exit()

