from typing import List

class Parameters:
	"""
	A class containing the model's hyperparameters.
	Default values are provided for the Atari Frostbite environment.
	"""

	DATABASE_IP = "10.0.0.6"

	#: HOW MANY TIME STEPS TO RUN THE ENVIRONMENT FOR BEFORE TIMING OUT
	MAX_NUM_STEPS = 1000

	#: The quantity of root teams per generation.
	POPULATION_SIZE: int = 360

	#: The number of programs generated in the first generation.
	INITIAL_PROGRAM_POPULATION: int = 1000

	#: The percentage of teams removed after a generation is completed.
	POPGAP: float = 0.5
	
	#: The list of possible actions for the environment.
	ACTIONS: List = [ "LEFT", "RIGHT" ]

	#: The name of the environment.
	ENVIRONMENT: str = "CartPole-v1"

	#: The size of the state/observation space.
	NUM_OBSERVATIONS: int = 4

	#: The number of registers that all programs have.
	NUM_REGISTERS: int = 8

	#: The probability of deleting an instruction.
	DELETE_INSTRUCTION_PROBABILITY: float = 0.7

	#: The probability of adding an instruction.
	ADD_INSTRUCTION_PROBABILITY: float = 0.7

	#: The probability of swapping an instruction.
	SWAP_INSTRUCTION_PROBABILITY: float = 1.0

	#: The probability of mutating an instruction.
	MUTATE_INSTRUCTION_PROBABILITY: float = 1.0

	#: The probability of adding a program to a team.
	ADD_PROGRAM_PROBABILITY: float = 0.7

	#: The probabilty of deleting a program from a team.
	DELETE_PROGRAM_PROBABILITY: float = 0.7

	#: The probability of creating a new program and adding it to a team.
	NEW_PROGRAM_PROBABILITY: float = 0.2

	#: The probability of mutating a program in a team.
	MUTATE_PROGRAM_PROBABILITY: float = 0.2

	#: The probability of a program referencing another team.
	TEAM_POINTER_PROBABILITY: float = 0.5

	#: The maximum number of instructions a program may have.
	MAX_INSTRUCTION_COUNT: int = 96

	#: The maximum number of programs a team may have during creation.
	MAX_INITIAL_TEAM_SIZE: int = 5

	MAX_PROGRAM_COUNT: int = 200

	DIVERSITY_CACHE_SIZE: int = 50

	DIVERSITY_TAU: float = 0.0001

	#: On the nth generation, apply mutations multiple times in order to break past premature convergence.
	RAMPANT_MUTATION_INTERVAL: int = 10

	#: On rampant mutation rounds, apply N rounds of mutation. 
	RAMPANT_MUTATION_COUNT: int = 1
	
	
