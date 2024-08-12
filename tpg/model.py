import math
import pprint
import time

from db.database import Database
from tpg.program import Program
from tpg.team import Team
from tpg.mutator import Mutator
from parameters import Parameters

import pickle
import os

import matplotlib.pyplot as plt

import random
from typing import List, Tuple, Dict
import numpy as np
from uuid import uuid4

from collections import deque


class Model:
    """ The Model class wraps all Tangled Program Graph functionality into an easy-to-use class. """

    def __init__(self):
        #: The pool of available (competitive) programs
        self.programPopulation: List[Program] = [Program() for _ in range(Parameters.INITIAL_PROGRAM_POPULATION)]

        #: The pool of competitive teams
        self.teamPopulation: List[Team] = [Team(self.programPopulation) for _ in range(Parameters.POPULATION_SIZE)]

    def get_team(self, team_id: str) -> Team:
        for team in self.teamPopulation:
            if team_id == str(team.id):
                return team
        raise Exception("Team was not found in the team population")

    def save(self, filename: str) -> None:
        """
		Saves a model by serializing with Pickle
		Individual teams can't be saved because teams reference other teams.
		"""
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "wb+") as f:
            pickle.dump(self, f)

    @staticmethod
    def load(filename) -> "Model":
        """
		Loads a model from a serialized Pickle file
		:param filename: the file path to the model being loaded.
		"""

        with open(filename, "rb") as f:
            return pickle.load(f)

    def get_survivor_ids(self, run_id, generation) -> List:
        survivor_count = math.floor(Parameters.POPGAP * Parameters.POPULATION_SIZE)

        sorted_team_ids = Database.get_ranked_teams(run_id, generation).sort_values('rank')['team_id']
        survivor_ids = [str(team_id) for team_id in sorted_team_ids[:survivor_count].to_list()]
        return survivor_ids

    def repopulate(self, run_id, generation):

        cached_observations = Database.get_diversity_cache(run_id)

        while len(Database.get_root_teams(run_id)) < Parameters.POPULATION_SIZE:
            original = random.choice(self.teamPopulation)
            clone = original.copy()

            if generation % 10 == 0:
                for _ in range(10):
                    Mutator.mutateTeam(self.programPopulation, self.teamPopulation, clone, run_id)

            profile = []
            for observation in cached_observations:
                profile.append(Parameters.ACTIONS.index(clone.getAction(self.teamPopulation, observation, visited=[])))

            if any(np.array_equal(np.array(profile), np.array(cached_profile)) for cached_profile in
                   Database.get_diversity_profiles(run_id)):

                # mutate four times??
                for _ in range(4):
                    Mutator.mutateTeam(self.programPopulation, self.teamPopulation, clone, run_id)

                # Regenerate the profile
                profile = []
                for observation in cached_observations:
                    profile.append(
                        Parameters.ACTIONS.index(clone.getAction(self.teamPopulation, observation, visited=[])))

            print("Adding diversity profiles now")
            Database.add_profile(run_id, clone, time.time(), profile)

            self.teamPopulation.append(clone)
            print("Adding cloned team now")
            Database.add_team(run_id, clone)
