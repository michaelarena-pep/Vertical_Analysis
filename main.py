import sys
import importlib

def run_bad_reason_cleaner():
	from src.bad_reason_cleaner import count_long_fields
	count_long_fields()
	
def perplexity_call():
	from src.website_info_perplexity import load_prompt, async_call_perplexity_client, main
	main()
	
def gpt_vert_call():
    from src.assign_vert import main
    main()

def url_cleaner():
	from src.clean_urls import save_progress, main
	main()

if __name__ == '__main__':
	# print('Running url_cleaner...')
	# url_cleaner()
	print('Running website_info_perplexity...')
	perplexity_call()
	# print('Assigning Verticals...')
	# gpt_vert_call()
	# print('Running bad_reason_cleaner...')
	# run_bad_reason_cleaner()