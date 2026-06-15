/ receiver.q — kdb+ market data aggregator
/ Run: q receiver.q -p 5000
/ Tails inbox.csv written by receiver.cpp, computes top-of-book every second.

/ --- Raw quotes table ---
quotes:([] recv_ts_ns:`long$(); send_ts_ns:`long$(); sym:`$(); side:`char$(); px:`float$(); qty:`long$(); seq:`long$())

/ --- Insert function (for IPC if used) ---
insQuote:{[r] `quotes insert r}

/ --- File ingest state ---
.ingest.path:"inbox.csv"
.ingest.offset:0j
.ingest.lastSeq:()  / set of seen seqs for dedup

/ --- Top-of-book output table ---
book:([] sym:`$(); bid_px:`float$(); bid_qty:`long$(); ask_px:`float$(); ask_qty:`long$(); imbalance:`float$(); ts:`timestamp$())

/ --- Timer: runs every 1 second ---
/ Load new CSV lines and compute book

.z.ts:{
  / --- Ingest new lines from inbox.csv ---
  if[not () ~ key hsym `$.ingest.path;
    fh:hsym `$.ingest.path;
    raw:read0 fh;
    / Skip header and already-processed lines
    / Use line count as offset (simple approach)
    nlines:count raw;
    if[nlines > .ingest.offset;
      newlines:(1 + .ingest.offset) _ raw;  / skip header (line 0) and old lines
      if[0 = .ingest.offset; newlines:1 _ raw];  / first load: skip header
      if[0 < count newlines;
        / Parse CSV lines: recv_ts_ns,send_ts_ns,sym,side,px,qty,seq
        parsed:{
          f:"," vs x;
          if[7 > count f; :()];  / malformed
          (`long$"J"$f[0]; `long$"J"$f[1]; `$f[2]; first f[3]; `float$"F"$f[4]; `long$"J"$f[5]; `long$"J"$f[6])
          } each newlines;
        / Filter out empty (malformed) rows
        parsed:parsed where not ()~/: parsed;
        if[0 < count parsed;
          / Build table rows and insert, dedup by seq
          {[r]
            s:r 6;
            if[not s in .ingest.lastSeq;
              `quotes insert `recv_ts_ns`send_ts_ns`sym`side`px`qty`seq ! r;
              .ingest.lastSeq,:s;
              ];
            } each parsed;
          / Cap dedup set at 10000
          if[10000 < count .ingest.lastSeq;
            .ingest.lastSeq:(-5000)#.ingest.lastSeq];
        ];
      ];
      .ingest.offset:nlines;
    ];
  ];

  / --- Compute top-of-book ---
  now:"j"$.z.P;
  cutoff:now - 5000000000;  / 5 seconds in ns
  recent:select from quotes where recv_ts_ns > cutoff;

  if[0 < count recent;
    bids:select from recent where side = "B";
    asks:select from recent where side = "S";
    syms:distinct recent`sym;

    newbook:();
    {[s]
      sb:select from bids where sym = s;
      sa:select from asks where sym = s;

      bp:$[0 < count sb; max sb`px; 0n];
      ap:$[0 < count sa; min sa`px; 0n];

      / Best bid qty: latest row at best price
      bq:$[0 < count sb;
        last (select qty from sb where px = bp)`qty;
        0j];
      / Best ask qty: latest row at best price
      aq:$[0 < count sa;
        last (select qty from sa where px = ap)`qty;
        0j];

      / Imbalance
      sumB:$[0 < count sb; sum sb`qty; 0j];
      sumA:$[0 < count sa; sum sa`qty; 0j];
      imb:$[(sumB + sumA) > 0;
        (sumB - sumA) % (sumB + sumA);
        0f];

      newbook,: enlist `sym`bid_px`bid_qty`ask_px`ask_qty`imbalance`ts ! (s; bp; bq; ap; aq; imb; .z.P);
      } each syms;

    book::([sym:`$()] bid_px:`float$(); bid_qty:`long$(); ask_px:`float$(); ask_qty:`long$(); imbalance:`float$(); ts:`timestamp$());
    if[0 < count newbook;
      book::flip `sym`bid_px`bid_qty`ask_px`ask_qty`imbalance`ts ! flip newbook;
    ];
  ];
  }

/ Start 1-second timer
\t 1000

/ --- Startup message ---
-1 "receiver.q loaded. Tailing inbox.csv every 1s.";
-1 "  Queries:";
-1 "    count quotes      / total quotes ingested";
-1 "    book              / current top-of-book";
-1 "    select avg px, sum qty by sym, side from quotes  / summary";
