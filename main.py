import os

import polars as pl
import requests

FFLOGS_TOKEN = os.environ.get("FFLOGS_TOKEN", "")

DAMAGE_TYPE = {
    128: "Physical",
    1024: "Magical",
}


class FFLogsAPI:
    BASE_URL = "https://www.fflogs.com/api/v2/client"

    def __init__(self, report_id: str, fight_id: int, token: str = FFLOGS_TOKEN):
        self.token = token
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

        self.report_id = report_id
        self.fight_id = fight_id

    def gql_query(self, query, variables, operation_name):
        json_payload = {
            "query": query,
            "variables": variables,
            "operationName": operation_name,
        }
        response = requests.post(
            headers=self.headers, url=FFLogsAPI.BASE_URL, json=json_payload
        )
        response.raise_for_status()
        return response.json()


class IncomingDamage(FFLogsAPI):
    def __init__(
        self,
        damage_category_dict: dict[int, str],
        report_id: str,
        fight_id: int,
        token: str = FFLOGS_TOKEN,
        party_damage_ceiling: int = 300000,
        tank_damage_ceiling: int = 600000,
    ):
        super().__init__(report_id, fight_id, token)

        self.party_damage_ceiling = party_damage_ceiling
        self.tank_damage_ceiling = tank_damage_ceiling

        self.event_response = self.get_incoming_damage()

        self.start_timestamp = self._get_start_timestamp(self.event_response)

        self.party_table = self._get_party_table(self.event_response)
        self.tank_ids, self.non_tank_ids = self._get_role_ids(self.party_table)

        self.vuln_ids = self._get_vuln_ids(self.event_response)
        self.damage_category_table = self._get_damage_category_table(
            damage_category_dict
        )
        self.damage_events_table = self._get_damage_events_table(self.event_response)
        pass

    def get_incoming_damage(self):
        damage_events_query = """
        query DamageEvents($code: String!, $id: [Int]!) {
        reportData {
            report(code: $code) {
                    playerDetails(fightIDs: $id)
            buffTable: table(fightIDs: $id, dataType: Debuffs)
            # Used to get start time from `"type": "limitbreakupdate"`
            startingEvent: events(
                fightIDs: $id
                dataType: All
                limit: 50
            ) {
                data
                nextPageTimestamp
            }
            events(
                fightIDs: $id
                dataType: DamageTaken
                useAbilityIDs: false
                limit: 10000
            ) {
                data
                nextPageTimestamp
            }
            }
        }
        }
        """
        damage_event_vars = {"id": [self.fight_id], "code": self.report_id}
        return self.gql_query(damage_events_query, damage_event_vars, "DamageEvents")

    @staticmethod
    def _get_start_timestamp(event_response) -> int:
        # LB update seems to set the start time
        try:
            start_time = (
                pl.from_records(
                    event_response["data"]["reportData"]["report"]["startingEvent"][
                        "data"
                    ]
                )
                .filter(pl.col("type") == "limitbreakupdate")
                .select("timestamp")[0]
                .item()
            )
        # Fallback to first damage event if it doesn't exist for some reason
        except Exception:
            start_time = event_response["data"]["reportData"]["report"]["events"][
                "data"
            ][0]["timestamp"]

        return start_time

    @staticmethod
    def _get_party_table(event_response):
        players = event_response["data"]["reportData"]["report"]["playerDetails"][
            "data"
        ]["playerDetails"]

        player_info = []
        for roles in players.keys():
            player_info.extend(players[roles])

        return pl.from_records(player_info).select("name", "id", "icon")

    @staticmethod
    def _get_role_ids(party_table: pl.DataFrame) -> tuple[list[int], list[int]]:
        """Get lists role IDs for tanks and non-tanks."""
        TANKS = ("DarkKnight", "Gunbreaker", "Warrior", "Paladin")
        tank_ids = party_table.filter(pl.col("icon").is_in(TANKS))["id"].to_list()
        not_tank_ids = party_table.filter(~pl.col("icon").is_in(TANKS))["id"].to_list()

        return tank_ids, not_tank_ids

    @staticmethod
    def _get_vuln_ids(event_response: dict[str, dict]) -> list[int]:
        """Filter debuffs down to Vulnerability Up and get the ability ID."""
        debuff_records = event_response["data"]["reportData"]["report"]["buffTable"][
            "data"
        ]["auras"]
        vuln_records = [
            b for b in debuff_records if b["name"].lower() == "vulnerability up"
        ]

        return [v["guid"] for v in vuln_records]

    @staticmethod
    def _get_damage_category_table(party_damage_dict: dict) -> pl.DataFrame:
        ability_ids = []
        damage_categories = []
        is_tank_damages = []
        ability_names = []
        descriptions = []

        for ability_id, metadata in party_damage_dict.items():
            ability_ids.append(ability_id)
            # Handle both dict (new format) and simple value (old format)
            if isinstance(metadata, dict):
                damage_categories.append(metadata.get("damage_category"))
                is_tank_damages.append(metadata.get("is_tank_damage", False))
                ability_names.append(metadata.get("ability_name"))
                description = metadata.get("description")
                descriptions.append(description if description else None)
            else:
                # Fallback for backward compatibility
                damage_categories.append(metadata)
                is_tank_damages.append(False)
                ability_names.append(None)
                descriptions.append(None)

        return pl.DataFrame(
            {
                "ability_id": ability_ids,
                "damage_category": damage_categories,
                "is_tank_damage": is_tank_damages,
                "ability_name": ability_names,
                "description": descriptions,
            }
        )

    @staticmethod
    def make_buff_list_col() -> pl.Expr:
        """Convert buff string column to list[int] column."""
        array_buffs = pl.col("buffs").str.strip_chars_end(".").str.split(".")
        return array_buffs.cast(pl.List(pl.Int64)).alias("buff_list")

    @staticmethod
    def flag_vuln_status(vuln_ids: list[int]) -> pl.Expr:
        has_vuln = (
            pl.col("buff_list").list.eval(pl.element().is_in(vuln_ids)).list.any()
        )
        # No buffs gives null, meaning no vuln
        return has_vuln.fill_null(False)

    @staticmethod
    def flag_tank_player_id(tank_ids: list[int]) -> pl.Expr:
        """Flag if a targetID for damage received is a Tank."""
        return (
            pl.when(pl.col("targetID").is_in(tank_ids))
            .then(True)
            .otherwise(False)
            .alias("is_tank")
        )

    @staticmethod
    def assign_group_by_field() -> pl.Expr:
        return (
            pl.when(pl.col("damage_category") == "party")
            .then(pl.col("packetID"))
            .otherwise(pl.col("elapsed_seconds"))
        )

    @staticmethod
    def format_elapsed_time(seconds_col) -> pl.Expr:
        """Format elapsed time in MM:SS.sss."""
        secs = seconds_col % 60
        return pl.concat_str(
            ((seconds_col // 60).cast(int)).cast(str).str.zfill(2),
            pl.lit(":"),
            (secs.cast(int)).cast(str).str.zfill(2),
            pl.lit("."),
            ((secs * 1000 % 1000).cast(int)).cast(str).str.zfill(3),
        )

    def _get_damage_events_table(self, event_response) -> pl.DataFrame:
        """Create and process the incoming damage event table for analysis."""
        raw_damage_events_df = pl.from_records(
            event_response["data"]["reportData"]["report"]["events"]["data"]
        ).filter(pl.col("type") == "damage")

        # Get relevant columns
        damage_events_table = raw_damage_events_df.select(
            ((pl.col("timestamp") - self.start_timestamp) / 1000).alias(
                "elapsed_seconds"
            ),
            "packetID",
            "hitType",
            "amount",
            "unmitigatedAmount",
            "targetID",
            self.make_buff_list_col(),
            pl.col("ability").struct.field("name").alias("ability_name"),
            pl.col("ability").struct.field("guid").alias("ability_id"),
            pl.col("ability").struct.field("type").alias("damage_type_id"),
        ).with_columns(
            formatted_time=self.format_elapsed_time(pl.col("elapsed_seconds")),
            is_tank=self.flag_tank_player_id(self.tank_ids),
            is_vuln_damage=self.flag_vuln_status(self.vuln_ids),
            damage_type=pl.col("damage_type_id").replace_strict(
                DAMAGE_TYPE, default="unknown"
            ),
        )

        # Flag tank damage
        # Flag vuln stack damage (filter out)
        damage_events_table = damage_events_table.with_columns(
            is_tank=self.flag_tank_player_id(self.tank_ids),
            is_vuln_damage=self.flag_vuln_status(self.vuln_ids),
        )

        # Classify how the damage is received, affects how to group by:
        damage_events_table = damage_events_table.join(
            self.damage_category_table, on="ability_id", how="left"
        ).with_columns(group_by_field=self.assign_group_by_field())

        return damage_events_table

    def get_damaging_ability_names_and_ids(self):
        return (
            self.damage_events_table.select(
                "ability_name", "ability_id", "damage_type_id"
            )
            .unique()
            .sort("ability_id")
        )

    @staticmethod
    def _aggregate_incoming_damage(
        filtered_damage_events: pl.DataFrame,
    ) -> pl.DataFrame:
        return (
            filtered_damage_events.group_by("group_by_field")
            .agg(
                [
                    pl.col("elapsed_seconds").median(),
                    IncomingDamage.format_elapsed_time(
                        pl.col("elapsed_seconds").median()
                    ).alias("formatted_time"),
                    pl.col("ability_name").first(),
                    pl.col("unmitigatedAmount").median().cast(int),
                    pl.col("description").first(),
                    pl.col("damage_type").first(),
                    pl.col("damage_type_id").first(),
                ]
            )
            .sort("elapsed_seconds")
        )

    def get_incoming_damage_profile(
        self, filter_uncategorized_damage: bool = True
    ) -> pl.DataFrame:
        party_damage = self.damage_events_table.filter(
            ~pl.col("is_tank")
            & ~pl.col("is_tank_damage")
            & ~pl.col("is_vuln_damage")
            & pl.col("unmitigatedAmount").is_not_null()
            & (pl.col("unmitigatedAmount") < self.party_damage_ceiling)
        )

        if filter_uncategorized_damage:
            party_damage = party_damage.filter(pl.col("damage_category").is_not_null())

        incoming_damage_profile = self._aggregate_incoming_damage(party_damage)

        return incoming_damage_profile

    def get_incoming_tank_damage_profile(
        self, filter_uncategorized_events: bool = True, filter_vulns: bool = False
    ) -> pl.DataFrame:
        tank_damage = self.damage_events_table.filter(
            pl.col("is_tank")
            & pl.col("is_tank_damage")
            & pl.col("unmitigatedAmount").is_not_null()
            & (pl.col("unmitigatedAmount") < self.tank_damage_ceiling)
        )

        if filter_vulns:
            tank_damage = tank_damage.filter(~pl.col("is_vuln_damage"))

        return tank_damage.select(
            "elapsed_seconds",
            "formatted_time",
            "ability_name",
            "unmitigatedAmount",
            "targetID",
            "description",
            "damage_type",
            "damage_type_id",
        )

    def plot_party_damage(self, fight_name: str):
        """
        Create a bar chart of party damage over time.

        Args:
            fight_name: Name of the fight for the title

        Returns:
            plotly Figure object
        """
        import plotly.express as px

        party_damage_agg = self.get_incoming_damage_profile()

        fig = px.bar(
            party_damage_agg.with_columns(
                time=pl.datetime(2000, 1, 1)
                + pl.duration(seconds=pl.col("elapsed_seconds"))
            ),
            x="time",
            y="unmitigatedAmount",
            text="ability_name",
            labels={
                "time": "Fight duration",
                "unmitigatedAmount": "Avg. unmitigated damage (non-tank)",
                "ability_name": "Ability",
                "damage_type": "Damage Type",
                "description": "Description",
            },
            color="damage_type",
            color_discrete_map={"Magical": "#636EFA", "Physical": "#FFA15A"},
            height=600,
            hover_data=[
                "ability_name",
                "description",
                "unmitigatedAmount",
                "damage_type",
            ],
            title=f"{fight_name} Party damage",
        ).update_layout(xaxis_tickformat="%M:%S")

        fig.update_traces(width=3000)

        return fig

    def plot_tank_damage(self, fight_name: str, color_by_target: bool = False):
        """
        Create a bar chart of tank damage over time.

        Args:
            fight_name: Name of the fight for the title
            color_by_target: If True, color by targetID; if False, color by damage_type

        Returns:
            plotly Figure object
        """
        import plotly.express as px

        tank_damage_df = self.get_incoming_tank_damage_profile()

        if color_by_target:
            # Cast targetID to string for discrete colors
            tank_damage_df = tank_damage_df.with_columns(
                time=pl.datetime(2000, 1, 1)
                + pl.duration(seconds=pl.col("elapsed_seconds")),
                targetID=pl.col("targetID").cast(pl.Utf8),
            )
            color_col = "targetID"
            color_map = None
        else:
            tank_damage_df = tank_damage_df.with_columns(
                time=pl.datetime(2000, 1, 1)
                + pl.duration(seconds=pl.col("elapsed_seconds"))
            )
            color_col = "damage_type"
            color_map = {"Magical": "#636EFA", "Physical": "#FFA15A"}

        fig = px.bar(
            tank_damage_df,
            x="time",
            y="unmitigatedAmount",
            text="ability_name",
            labels={
                "damage_type": "Damage Type",
                "elapsed_seconds": "Fight duration (s)",
                "unmitigatedAmount": "Tank damage",
                "ability_name": "Ability",
                "targetID": "Target ID",
                "description": "Description",
            },
            height=600,
            color=color_col,
            color_discrete_map=color_map,
            hover_data=[
                "ability_name",
                "description",
                "unmitigatedAmount",
                "damage_type",
            ],
            title=f"{fight_name} tank damage",
        ).update_layout(xaxis_tickformat="%M:%S")

        fig.update_traces(width=2000)

        return fig


if __name__ == "__main__":
    import fights

    FFLOGS_TOKEN = os.environ.get("FFLOGS_TOKEN", "")

    logs = {
        "m9n": {
            "report_id": "2ajHBbTkpRfFxMJg",
            "fight_id": 5,
            "party_damage": fights.M9N,
        },
        "m10n": {
            "report_id": "KLypxHcraTdRzY7F",
            "fight_id": 3,
            "party_damage": fights.M10N,
        },
        "m11n": {
            "report_id": "vN4py2QhT91XKLHW",
            "fight_id": 55,
            "party_damage": {},
        },
        "m12n": {
            "report_id": "YCLr1Aj6FJ4yQXNH",
            "fight_id": 23,
            "party_damage": fights.M12N,
        },
    }

    fight = "m11n"

    report_id = logs[fight]["report_id"]
    fight_id = logs[fight]["fight_id"]

    # Use the updated fight schema
    party_damage = fights.M9N

    incoming_damage = IncomingDamage(
        party_damage,
        report_id=logs[fight]["report_id"],
        fight_id=logs[fight]["fight_id"],
        token=FFLOGS_TOKEN,
    )
