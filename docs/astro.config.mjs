// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	// Published on GitHub Pages: https://tosun-si.github.io/asgarde/
	site: 'https://tosun-si.github.io',
	base: '/asgarde',
	integrations: [
		starlight({
			title: 'Asgarde',
			description: 'Error handling and dead letter queues for Apache Beam, in Java, Kotlin and Python.',
			logo: { src: './src/assets/asgarde-logo.gif', alt: 'Asgarde' },
			favicon: '/favicon.gif',
			customCss: ['./src/styles/custom.css'],
			social: [
				{ icon: 'github', label: 'Asgarde Java on GitHub', href: 'https://github.com/tosun-si/asgarde' },
				{ icon: 'seti:python', label: 'Asgarde Python on GitHub', href: 'https://github.com/tosun-si/pasgarde' },
			],
			editLink: { baseUrl: 'https://github.com/tosun-si/asgarde/edit/main/docs/' },
			lastUpdated: true,
			sidebar: [
				{
					label: 'Getting started',
					items: [
						{ label: 'Why Asgarde', slug: 'getting-started/why-asgarde' },
						{ label: 'Installation', slug: 'getting-started/installation' },
						{ label: 'Quick start', slug: 'getting-started/quick-start' },
					],
				},
				{
					label: 'Concepts',
					items: [
						{ label: 'CollectionComposer', slug: 'concepts/collection-composer' },
						{ label: 'Failure', slug: 'concepts/failure' },
						{ label: 'Failure handling guarantees', slug: 'concepts/guarantees' },
						{ label: 'Failure metrics', slug: 'concepts/metrics' },
					],
				},
				{
					label: 'Java & Kotlin',
					items: [
						{ label: 'Transforms', slug: 'java/transforms' },
						{ label: 'Side inputs and lifecycle', slug: 'java/side-inputs-lifecycle' },
						{ label: 'Custom DoFn', slug: 'java/custom-dofn' },
						{ label: 'Coders', slug: 'java/coders' },
						{ label: 'Kotlin extensions', slug: 'java/kotlin' },
					],
				},
				{
					label: 'Python',
					items: [
						{ label: 'Operators', slug: 'python/operators' },
						{ label: 'Side inputs and lifecycle', slug: 'python/side-inputs-lifecycle' },
						{ label: 'Custom DoFn', slug: 'python/custom-dofn' },
					],
				},
				{
					label: 'Project',
					items: [
						{ label: 'Compatibility', slug: 'project/compatibility' },
						{ label: 'Roadmap', slug: 'project/roadmap' },
						{ label: 'Contributing', slug: 'project/contributing' },
					],
				},
			],
		}),
	],
});
