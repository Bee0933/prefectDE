install:
	# install dependencies
	pip install --upgrade pip &&\
		pip install -r requirements.txt 
format:
	# format python code with black
	black *.py 
lint:
	# check code syntaxes
	pylint --disable=R,C *.py 
