const fs = require('fs');
const pdfjsLib = require('pdfjs-dist/legacy/build/pdf.js');

const pdfPath = process.argv[2] || '../../docs/project-plan.pdf';
const mdPath = pdfPath.replace('.pdf', '.md');

async function main() {
    const dataBuffer = new Uint8Array(fs.readFileSync(pdfPath));
    const doc = await pdfjsLib.getDocument({ data: dataBuffer }).promise;

    let md = `# ${pdfPath.replace('.pdf', '')}\n\n`;

    for (let i = 1; i <= doc.numPages; i++) {
        const page = await doc.getPage(i);
        const content = await page.getTextContent();
        const items = content.items;
        let lastY = null;
        for (const item of items) {
            if (lastY !== null && Math.abs(item.transform[5] - lastY) > 2) {
                md += '\n';
            }
            md += item.str;
            lastY = item.transform[5];
        }
        md += '\n\n---\n\n';
    }

    fs.writeFileSync(mdPath, md, 'utf-8');
    console.log(`Converted "${pdfPath}" to "${mdPath}"`);
    console.log(`Pages: ${doc.numPages}`);
}

main().catch(err => {
    console.error('Error:', err.message);
    process.exit(1);
});
