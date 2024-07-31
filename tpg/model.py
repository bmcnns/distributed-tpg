import math
import pprint

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
		self.programPopulation: List[Program] = [ Program() for _ in range(Parameters.INITIAL_PROGRAM_POPULATION)]

		#: The pool of competitive teams 
		self.teamPopulation: List[Team] = [ Team(self.programPopulation) for _ in range(Parameters.POPULATION_SIZE)]

	def cleanProgramPopulation(self) -> None:
		"""
		Used internally. After teams are removed from the population, clean up any programs
		that are no longer in use, since they are no longer competitive.
		"""
		inUseProgramIds: List[str] = []
		for team in self.teamPopulation:
			for program in team.programs:
				inUseProgramIds.append(program.id)

		for program in self.programPopulation:
			if program.id not in inUseProgramIds:
				self.programPopulation.remove(program)

	def select(self) -> None:
		"""
		After agents (root teams) are evaluated in a generation, this method is called
		to sort them by fitness and remove POPGAP percentage of the total root team population.
		The program population is cleaned after teams are removed.
		"""
	
		sortedTeams: List[Team] = list(sorted(self.getRootTeams(), key=lambda team: team.getFitness()))

		remainingTeamsCount: int = int(Parameters.POPGAP * len(self.getRootTeams()))

		for team in sortedTeams[:remainingTeamsCount]:
			
			if team.luckyBreaks > 0:
				team.luckyBreaks -= 1
				print(f"Tried to remove team {team.id} but they had a lucky break! {team.getFitness()} (remaining breaks: {team.luckyBreaks})")
			else:
				if team.referenceCount == 0:
					print(f"Removing team {team.id} with fitness {team.getFitness()}")
					self.teamPopulation.remove(team)

		self.cleanProgramPopulation() 

	def evolve(self, generation: int) -> None:
		"""
		After removing the uncompetitive teams, clone the remaining competitive root teams
		and apply mutations to the clones until the discarded population is replaced.
		"""
		while len(self.getRootTeams()) < Parameters.POPULATION_SIZE:
			team = random.choice(self.getRootTeams()).copy()

			Mutator.mutateTeam(self.programPopulation, self.teamPopulation, team)

			self.teamPopulation.append(team)

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

	def get_survivor_ids(self, generation) -> List:
		survivor_count = math.floor(Parameters.POPGAP * Parameters.POPULATION_SIZE)

		sorted_team_ids = Database.get_ranked_teams(generation).sort_values('rank')['team_id']
		survivor_ids = [ str(team_id) for team_id in sorted_team_ids[:survivor_count].to_list()]
		return survivor_ids

	def repopulate(self, generation):

		print("Diversity cache")
		pprint.pp(Database.get_diversity_cache())
		cached_observations = Database.get_diversity_cache()

		while len(Database.get_root_teams()) < Parameters.POPULATION_SIZE:
			original = random.choice(self.teamPopulation)
			clone = original.copy()

			if generation % 10 == 0:
				for _ in range(10):
					print("RAMPANT MUTATION")
					Mutator.mutateTeam(self.programPopulation, self.teamPopulation, clone)

			profile = []
			for observation in cached_observations:
				profile.append(Parameters.ACTIONS.index(clone.getAction(self.teamPopulation, observation, visited=[])))

			print(f"Profile generated for clone {clone.id}")

            
			if any(np.array_equal(np.array(profile), np.array(cached_profile)) for cached_profile in Database.get_diversity_profiles()):
				print(f"Profile for clone {clone.id} already in diversity cache.")

				# mutate four times??
				for _ in range(4):
					Mutator.mutateTeam(self.programPopulation, self.teamPopulation, clone)

				# Regenerate the profile
				profile = []
				for observation in cached_observations:
					profile.append(Parameters.ACTIONS.index(clone.getAction(self.teamPopulation, observation, visited=[])))

			Database.add_profile(clone, profile)

			self.teamPopulation.append(clone)
			Database.add_team(clone)