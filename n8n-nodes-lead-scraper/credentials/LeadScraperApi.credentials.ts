import {
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class LeadScraperApi implements ICredentialType {
	name = 'leadScraperApi';
	displayName = 'Lead Scraper API';
	documentationUrl = '';

	properties: INodeProperties[] = [
		{
			displayName: 'Base URL',
			name: 'baseUrl',
			type: 'string',
			default: 'http://localhost:5000',
			placeholder: 'http://localhost:5000',
			description: 'The base URL of your Lead Scraper instance',
		},
	];
}
