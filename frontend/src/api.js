export async function getJSON(url) {
  const r = await fetch(url)
  if (!r.ok) throw new Error(await r.text())
  return r.json()
}

export async function postJSON(url, body = {}, method = 'POST') {
  const r = await fetch(url, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) throw new Error(await r.text())
  return r.json()
}
