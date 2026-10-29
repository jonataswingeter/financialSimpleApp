package com.financialsimpleapp.transfer_service.transfer.application.port.out;

import com.financialsimpleapp.transfer_service.transfer.domain.LedgerEntry;

/** The append-only ledger: entries are written once and never changed. */
public interface LedgerRepository {

  void append(LedgerEntry entry);
}
