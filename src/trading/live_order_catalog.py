"""Verified live identities to the bound read catalog; no HTTP, keys or activation."""

from trading.known_orders import CatalogError, KnownOrder
from trading.live_journal import LiveOrderJournal


class LiveCatalogError(ValueError):
    """Fixed local reasons only."""


class LiveOrderCatalogSource:
    def __init__(self, posts, catalog, *, clock):
        self.posts, self.catalog, self.clock = posts, catalog, clock
        self.binding = self._binding()

    def _binding(self):
        post = self.posts.snapshot()
        binding = self.posts.execution_binding()
        if (
            binding is None
            or post["scope"] != self.catalog.scope
            or self.posts.reads.stream_binding() != self.catalog.control_instance
        ):
            raise LiveCatalogError("live_catalog_binding_required")
        return binding

    def refresh(self, *, expected_head=None):
        journal = None
        try:
            if self._binding() != self.binding:
                raise LiveCatalogError("live_catalog_binding_changed")
            journal = LiveOrderJournal(self.binding["path"], self.posts, clock=self.clock)
            export = journal.catalog_orders()
            post = self.posts.snapshot()
            if (
                export["instance"] != self.binding["instance"]
                or export["read_instance"] != post["read_instance"]
                or export["post_instance"] != post["instance"]
                or export["scope"] != self.catalog.scope
            ):
                raise LiveCatalogError("live_catalog_binding_changed")
            state, known = self.catalog._read()
            head = state.head
            if expected_head is not None and expected_head != head:
                raise LiveCatalogError("live_catalog_checkpoint_changed")
            clients = {d.order.intent.client_id: d.order for d in known.values()}
            pending = []
            for item in export["orders"]:
                order = KnownOrder.model_validate(item["order"])
                existing = known.get(order.order_id)
                if (
                    existing is not None
                    and existing.order != order
                    or order.intent.client_id in clients
                    and clients[order.intent.client_id] != order
                ):
                    raise CatalogError("catalog_order_conflict")
                if existing is None:
                    pending.append((order, item["source_ref"]))
            registered = 0
            for order, source_ref in pending:
                # Only a head race is retried. Identity conflicts and damage stop the source.
                for attempt in range(3):
                    try:
                        result = self.catalog.register(
                            order,
                            source_ref=source_ref,
                            expected_head=head,
                            intent_confirmed=True,
                        )
                        break
                    except CatalogError as error:
                        if str(error) != "catalog_head_changed":
                            raise
                        if attempt == 2:
                            raise
                        head = self.catalog.snapshot()["head"]
                head = result["head"]
                registered += not result["already_known"]
            return {
                **self.catalog.snapshot(),
                "live_instance": export["instance"],
                "registered_count": registered,
                "identified_count": len(export["orders"]),
                "unidentified_client_ids": export["unidentified_client_ids"],
            }
        except LiveCatalogError as error:
            if str(error) == "live_catalog_checkpoint_changed":
                raise  # A stale manual checkpoint makes no mutation and does not stop trading.
            self._halt(journal)
            raise LiveCatalogError("live_catalog_source_failed") from None
        except (ValueError, OSError, KeyError, TypeError, AttributeError, ArithmeticError):
            self._halt(journal)
            raise LiveCatalogError("live_catalog_source_failed") from None

    @staticmethod
    def _halt(journal):
        if journal is not None:
            try:
                journal.halt()
            except Exception:
                pass  # Corrupt live storage also refuses subsequent dispatch.

    def halt(self):
        """A bound read failure stops live dispatch, including when no order GET completed."""
        try:
            self._halt(LiveOrderJournal(self.binding["path"], self.posts, clock=self.clock))
        except (ValueError, OSError):
            pass

    def lookup(self, ids):
        self.refresh()
        try:
            return self.catalog.lookup(ids)
        except CatalogError:
            self.halt()
            raise LiveCatalogError("live_catalog_order_unknown") from None

    def verify_reports(self, reports):
        journal = None
        try:
            if self._binding() != self.binding:
                raise LiveCatalogError("live_catalog_binding_changed")
            journal = LiveOrderJournal(self.binding["path"], self.posts, clock=self.clock)
            journal.catalog_orders()  # Revalidate the saved source after the GETs.
            journal.verify_catalog_evidence(reports)
        except (ValueError, OSError, KeyError, TypeError, AttributeError, ArithmeticError):
            self._halt(journal)
            raise LiveCatalogError("live_catalog_read_failed") from None
