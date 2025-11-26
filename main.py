import sys
import importlib

def run_bad_reason_cleaner():
	from utils.bad_reason_cleaner import count_long_fields
	count_long_fields()
	
def perplexity_call():
	from src.website_info_perplexity import load_prompt, async_call_perplexity_client, main
	main()
	
def gpt_vert_call():
    from src.assign_vert import main
    main()

def url_cleaner():
	from utils.clean_urls import main
	main()

def web_info_parser():
	from utils.web_parser import parse_info, main
	main()

def comp_type_classifier():
	from src.company_type import main
	main()

def sub_vertical_classifier():
	from src.sub_vertical import main
	main()

def clear_error_sub_vertical():
	from utils.reason_timeout import main
	main()

if __name__ == '__main__':
	#Purpose: Cleans URLS to a homepage format since HubSpot websites are formatted inconsistently.
	print('Running url_cleaner...')
	url_cleaner()
	# Purpose: Calls Perplexity API to get website information.
	print('Running website_info_perplexity...')
	perplexity_call()
	# Purpose: For inputs that included all of the model reasoning given perpetual loop from a bad URL, turn these values to N/A.
	print('Running bad_reason_cleaner...')
	run_bad_reason_cleaner()
	# Purpose: Parse Website info for explicit categories.
	print('Parsing Website Information...')
	web_info_parser()
	# Purpose: Calls GPT API to assign company type given the respective website Information that was pulled.
	print('Assigning Company Type...')
	comp_type_classifier()
	# Purpose: Calls GPT API to assign verticals given the respective website Information that was pulled.
	print('Assigning Verticals...')
	gpt_vert_call()
	# Purpose: Calls GPT API to assign sub-verticals given the respective website Information that was pulled.
	print('Assigning Sub-Verticals...')
	sub_vertical_classifier()
	# Purpose: Clean Error Requests
	print('Cleaning Sub-Verticals...')
	clear_error_sub_vertical()