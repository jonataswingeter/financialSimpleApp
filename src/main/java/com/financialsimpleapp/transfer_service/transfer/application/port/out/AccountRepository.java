package com.financialsimpleapp.transfer_service.transfer.application.port.out;

import com.financialsimpleapp.transfer_service.transfer.domain.Account;
import java.util.List;
import java.util.UUID;

/** Persists the {@link Account} aggregate. */
public interface AccountRepository {

  /**
   * Loads the accounts that exist and locks them until the transaction ends, always in the same
   * order (by id) so that concurrent transfers between the same accounts cannot deadlock.
   */
  List<Account> lockByIds(UUID firstAccountId, UUID secondAccountId);

  void saveBalance(Account account);
}
