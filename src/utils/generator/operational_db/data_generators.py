import random
import uuid
from decimal import Decimal
from typing import Any, Dict, List, Tuple


from src.utils.generator.common.base_generator import BaseGenerator


class OrganizerGenerator(BaseGenerator):
    def populate(self, cursor: Any, **dependencies: Any) -> List[str]:
        rows = []
        for _ in range(self.settings.num_organizers):
            organizer_id = str(uuid.uuid4())
            rows.append((
                organizer_id,
                self.fake.company(),
                self.fake.unique.company_email(),
                self.fake.phone_number()[:50],
                self.fake.address().replace("\n", ", "),
            ))

        cursor.executemany(
            """INSERT INTO organizer (organizer_id, name, email, phone, address)
               VALUES (%s, %s, %s, %s, %s)""", rows
        )
        print(f"{len(rows)} organizers populated.")
        return [r[0] for r in rows]


class ResellerGenerator(BaseGenerator):
    def populate(self, cursor: Any, **dependencies: Any) -> List[str]:
        rows = []
        for _ in range(self.settings.num_resellers):
            reseller_id = str(uuid.uuid4())
            region = random.choice(self.settings.regions)
            rows.append((
                reseller_id,
                f"{self.fake.company()} Tickets",
                self.fake.unique.email(),
                self.fake.phone_number()[:50],
                self.fake.city(),
                self.fake.country(),
                region,
            ))

        cursor.executemany(
            """INSERT INTO reseller (reseller_id, name, email, phone, city, country, region)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""", rows
        )
        print(f"{len(rows)} resellers populated.")
        return [r[0] for r in rows]


class CommissionAgreementGenerator(BaseGenerator):
    def populate(self, cursor: Any, **dependencies: Any) -> Dict[Tuple[str, str], str]:
        organizer_ids = dependencies["organizer_ids"]
        reseller_ids = dependencies["reseller_ids"]

        rows = []
        seen = set()
        for reseller_id in reseller_ids:
            sampled_organizers = random.sample(organizer_ids, k=min(3, len(organizer_ids)))
            for organizer_id in sampled_organizers:
                if (organizer_id, reseller_id) in seen:
                    continue
                seen.add((organizer_id, reseller_id))
                rows.append((
                    str(uuid.uuid4()),
                    organizer_id,
                    reseller_id,
                    Decimal(str(random.choice(self.settings.commission_rates))),
                ))

        cursor.executemany(
            """INSERT INTO commission_agreement (agreement_id, organizer_id, reseller_id, commission_rate)
               VALUES (%s, %s, %s, %s)""", rows
        )
        print(f"{len(rows)} commission agreements populated.")
        return {(r[1], r[2]): r[0] for r in rows}


class CustomerGenerator(BaseGenerator):
    def populate(self, cursor: Any, **dependencies: Any) -> List[str]:
        rows = []
        for _ in range(self.settings.num_customers):
            rows.append((
                str(uuid.uuid4()),
                self.fake.first_name(),
                self.fake.last_name(),
                self.fake.unique.email(),
                self.fake.phone_number()[:50],
                random.choice(self.settings.regions),
            ))

        cursor.executemany(
            """INSERT INTO customer (customer_id, first_name, last_name, email, phone, region)
               VALUES (%s, %s, %s, %s, %s, %s)""", rows
        )
        print(f"{len(rows)} customers populated.")
        return [r[0] for r in rows]


class EventGenerator(BaseGenerator):
    def populate(self, cursor: Any, **dependencies: Any) -> List[Tuple[str, str]]:
        organizer_ids = dependencies["organizer_ids"]
        rows = []
        for _ in range(self.settings.num_events):
            rows.append((
                str(uuid.uuid4()),
                random.choice(organizer_ids),
                f"{self.fake.catch_phrase()} {random.choice(['2019', '2020'])}",
                random.choice(self.settings.event_types),
                f"{self.fake.city()} Arena",
                random.choice(self.settings.regions),
                self._generate_random_date(),
            ))

        cursor.executemany(
            """INSERT INTO event (event_id, organizer_id, name, type, venue, region, event_date)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""", rows
        )
        print(f"{len(rows)} events populated.")
        return [(r[0], r[1]) for r in rows]


class TicketGenerator(BaseGenerator):
    def populate(self, cursor: Any, **dependencies: Any) -> List[Tuple[str, str]]:
        events = dependencies["events"]
        rows = []
        for _ in range(self.settings.num_tickets):
            event_id, _ = random.choice(events)
            face_value = round(random.uniform(15, 500), 2)
            rows.append((
                str(uuid.uuid4()),
                event_id,
                random.choice(self.settings.ticket_categories),
                Decimal(str(face_value)),
            ))

        cursor.executemany(
            """INSERT INTO ticket (ticket_id, event_id, category, face_value)
               VALUES (%s, %s, %s, %s)""", rows
        )
        print(f"{len(rows)} tickets populated.")
        return [(r[0], r[1]) for r in rows]


class SaleGenerator(BaseGenerator):
    def populate(self, cursor: Any, **dependencies: Any) -> None:
        tickets = dependencies["tickets"]
        events = dependencies["events"]
        customer_ids = dependencies["customer_ids"]
        reseller_ids = dependencies["reseller_ids"]
        agreements = dependencies["agreements"]
        count = dependencies["count"]
        source = dependencies.get("source", "platform")

        event_organizer_map = {event_id: org_id for event_id, org_id in events}
        rows = []

        for _ in range(count):
            ticket_id, event_id = random.choice(tickets)
            organizer_id = event_organizer_map[event_id]
            customer_id = random.choice(customer_ids)
            qty = random.randint(1, 6)
            unit_price = round(random.uniform(15, 500), 2)
            total = round(unit_price * qty, 2)

            use_reseller = random.random() < 0.60
            reseller_id = None
            agreement_id = None
            commission = Decimal("0")

            if use_reseller:
                eligible_resellers = [
                    (rid, agreements[(organizer_id, rid)])
                    for rid in reseller_ids if (organizer_id, rid) in agreements
                ]
                if eligible_resellers:
                    reseller_id, agreement_id = random.choice(eligible_resellers)
                    cursor.execute(
                        "SELECT commission_rate FROM commission_agreement WHERE agreement_id = %s",
                        (agreement_id,),
                    )
                    rate = cursor.fetchone()[0]
                    commission = round(Decimal(str(total)) * rate / 100, 2)
                else:
                    use_reseller = False

            rows.append((
                str(uuid.uuid4()),
                ticket_id,
                customer_id,
                organizer_id,
                reseller_id,
                agreement_id,
                random.choice(self.settings.channels),
                qty,
                Decimal(str(unit_price)),
                Decimal(str(total)),
                commission,
                source,
                self._generate_random_date(),
            ))

        cursor.executemany(
            """INSERT INTO sale (
                sale_id, ticket_id, customer_id, organizer_id, reseller_id,
                agreement_id, channel, quantity, unit_price, total_amount,
                commission_amount, source, sale_date
               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rows
        )
        print(f"{len(rows)} sales populated from source: '{source}'.")
